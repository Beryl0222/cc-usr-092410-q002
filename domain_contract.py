"""医学教学数据沙箱的领域契约测试。

覆盖需求中的全部治理承诺：
限时切片与脱敏、披露检查与小样本阻断、跨校到期收权、
勘误/升级前向影响、同意撤回即时效力、已评分作业指纹冻结、
教师复现与身份隔离、结论四要素溯源、快照身份不可变（异内容覆盖拒绝、
精确重放、并发首次提交、坏快照恢复、乱序/循环替代、沿链勘误与追溯）。
"""

import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from domain import (
    EXPORT_APPROVED,
    EXPORT_BLOCKED,
    REJECT_CHECKSUM,
    REJECT_CONFLICT,
    REJECT_CORRUPT,
    REJECT_REPLACEMENT,
    STATUS_GRADED,
    STATUS_REVOKED,
    STATUS_SANDBOX,
    AuthorizationError,
    Sandbox,
    SandboxError,
    SnapshotConflict,
    digest,
)

SEED = "fixtures/seed.json"
T = lambda s: datetime.fromisoformat(s)  # noqa: E731

# 一组独立的测试快照，避免与种子两版相互干扰
V1_ROWS = [{"patient_id": "P-1", "age": 40, "diagnosis": "AML-M2"}]
V1B_ROWS = [{"patient_id": "P-1", "age": 41, "diagnosis": "AML-M2"}]
V1_ROWS_KEY_SHUFFLED = [{"diagnosis": "AML-M2", "patient_id": "P-1", "age": 40}]
IDENTITY = ["patient_id"]
T1 = T("2026-02-20T00:00:00")
T2 = T("2026-05-10T00:00:00")
T3 = T("2026-06-10T00:00:00")


class SeedTest(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox.from_seed(SEED)

    def test_shared_case_enters_two_courses(self):
        case = self.box.cases["CASE-AML"]
        self.assertEqual(set(case["courses"]), {"C-LOCAL", "C-CROSS"})

    def test_six_ledgers_are_separate(self):
        # 六类分账各自独立存放，不混入一个总表
        for ledger in (self.box.cases, self.box.snapshots, self.box.consents,
                       self.box.policies, self.box.environment_versions,
                       self.box.assignments):
            self.assertGreater(len(ledger), 0)
        # 病例账不含数据行，也不含作业结论——分账不混存
        self.assertNotIn("rows", self.box.cases["CASE-AML"])
        self.assertNotIn("conclusion", self.box.cases["CASE-AML"])


class TimeLimitedSliceTest(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox.from_seed(SEED)
        self.now = T("2026-04-01T09:00:00")

    def test_slice_is_deidentified_and_objective_scoped(self):
        session = self.box.issue_slice("S-LIN", "TASK-LOCAL-Q1", self.now)
        self.assertEqual(session["status"], STATUS_SANDBOX)
        row = session["rows"][0]
        # 只含教学目标字段，且身份字段永不下发、城市被丢弃
        self.assertEqual(set(row), {"age", "diagnosis", "cell_type", "marker"})
        self.assertNotIn("patient_id", row)
        self.assertNotIn("city", row)
        # 年龄按十岁段泛化
        self.assertEqual(row["age"], "40-49")
        self.assertTrue(session["expires_at"] > self.now)

    def test_slice_expires(self):
        session = self.box.issue_slice("S-LIN", "TASK-LOCAL-Q1", self.now,
                                       ttl_minutes=240)
        after = session["expires_at"]
        dead = self.box._session_live(session, after)
        self.assertEqual(dead, "切片过期")

    def test_student_from_other_course_cannot_get_slice(self):
        # 王学生只注册跨校课程，不能取得本校课程切片
        with self.assertRaises(AuthorizationError):
            self.box.issue_slice("S-WANG", "TASK-LOCAL-Q1", self.now)


class DisclosureTest(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox.from_seed(SEED)
        self.now = T("2026-04-01T09:00:00")
        self.session = self.box.issue_slice("S-LIN", "TASK-LOCAL-Q1", self.now)

    def _export(self, export_id, groups, columns):
        return self.box.request_export(
            export_id, "S-LIN", self.session["id"], groups, columns, self.now,
            assignment_id="AS-LIN-01")

    def test_small_sample_group_is_blocked(self):
        # AML-M5 只有 3 例，低于 k≥5
        record = self._export("EXP-1", [{"key": "AML-M5", "count": 3}],
                              ["diagnosis", "cell_type"])
        self.assertEqual(record["decision"], EXPORT_BLOCKED)
        self.assertTrue(any("小样本" in reason for reason in record["reasons"]))

    def test_compliant_export_is_approved(self):
        record = self._export("EXP-2", [{"key": "AML-M2", "count": 5}],
                              ["diagnosis", "cell_type", "age"])
        self.assertEqual(record["decision"], EXPORT_APPROVED)
        self.assertEqual(record["reasons"], [])

    def test_identity_column_is_blocked_even_with_enough_rows(self):
        record = self._export("EXP-3", [{"key": "AML-M2", "count": 8}],
                              ["diagnosis", "patient_name"])
        self.assertEqual(record["decision"], EXPORT_BLOCKED)
        self.assertTrue(any("身份字段" in reason for reason in record["reasons"]))

    def test_export_after_slice_expiry_is_blocked(self):
        later = T("2026-04-01T14:00:00")  # 默认 240 分钟后
        record = self.box.request_export(
            "EXP-4", "S-LIN", self.session["id"],
            [{"key": "AML-M2", "count": 5}], ["diagnosis"], later)
        self.assertEqual(record["decision"], EXPORT_BLOCKED)
        self.assertIn("切片过期", record["reasons"])


class CrossCourseWithdrawalTest(unittest.TestCase):
    """同一病例进入两个课程，教学中途撤回同意。"""

    WITHDRAW_AT = T("2026-05-05T12:00:00")

    def setUp(self):
        self.box = Sandbox.from_seed(SEED)
        day_before = T("2026-05-04T09:00:00")
        self.local_session = self.box.issue_slice(
            "S-LIN", "TASK-LOCAL-Q1", day_before, ttl_minutes=2880)
        self.cross_session = self.box.issue_slice(
            "S-WANG", "TASK-CROSS-Q1", day_before, ttl_minutes=2880)
        # 撤回前：合规导出曾获批；小样本导出在披露队列
        self.box.request_export(
            "EXP-OK", "S-LIN", self.local_session["id"],
            [{"key": "AML-M2", "count": 5}], ["diagnosis"], day_before)
        self.box.request_export(
            "EXP-SMALL", "S-WANG", self.cross_session["id"],
            [{"key": "AML-M5", "count": 3}], ["diagnosis"], day_before)

    def test_withdrawal_blocks_small_sample_and_live_exports(self):
        self.assertEqual(self.box.exports["EXP-SMALL"]["decision"], EXPORT_BLOCKED)
        self.assertEqual(self.box.exports["EXP-OK"]["decision"], EXPORT_APPROVED)
        self.box.withdraw_consent("CONS-AML-TEACH", self.WITHDRAW_AT)
        # 小样本导出保持阻断；曾获批的导出因撤回立即失效
        self.assertEqual(self.box.exports["EXP-SMALL"]["decision"], EXPORT_BLOCKED)
        self.assertEqual(self.box.exports["EXP-OK"]["decision"], EXPORT_BLOCKED)
        self.assertIn("同意撤回", self.box.exports["EXP-OK"]["reasons"])

    def test_withdrawal_revokes_sessions_in_both_courses(self):
        self.box.withdraw_consent("CONS-AML-TEACH", self.WITHDRAW_AT)
        self.assertEqual(self.local_session["status"], STATUS_REVOKED)
        self.assertEqual(self.cross_session["status"], STATUS_REVOKED)
        # 撤回后两门课都不能再取切片
        with self.assertRaises(AuthorizationError):
            self.box.issue_slice("S-LIN", "TASK-LOCAL-Q1", self.WITHDRAW_AT)
        with self.assertRaises(AuthorizationError):
            self.box.issue_slice("S-WANG", "TASK-CROSS-Q1", self.WITHDRAW_AT)

    def test_affected_assignments_are_listed_across_both_courses(self):
        report = self.box.withdraw_consent("CONS-AML-TEACH", self.WITHDRAW_AT)
        ids = {row["assignment_id"] for row in report["assignments"]}
        self.assertEqual(ids, {"AS-LIN-01", "AS-WANG-01", "AS-GAO-01"})
        courses = {row["course_id"] for row in report["assignments"]}
        self.assertEqual(courses, {"C-LOCAL", "C-CROSS"})

    def test_graded_work_keeps_fingerprint_and_is_flagged(self):
        self.box.withdraw_consent("CONS-AML-TEACH", self.WITHDRAW_AT)
        lin = self.box.assignments["AS-LIN-01"]
        self.assertEqual(lin["status"], STATUS_GRADED)
        self.assertEqual(lin["grade"], "A")
        self.assertEqual(lin["conclusion"],
                         "AML-M2 组原始粒细胞占比高，CD34 阳性为主")
        self.assertEqual(lin["fingerprint"]["snapshot"]["version"], 1)
        self.assertEqual(lin["fingerprint"]["environment"]["version"], 1)
        types = {flag["type"] for flag in lin["risk_flags"]}
        self.assertIn("同意撤回", types)

    def test_new_small_sample_request_after_withdrawal_stays_blocked(self):
        self.box.withdraw_consent("CONS-AML-TEACH", self.WITHDRAW_AT)
        # 撤回后仍在试图导出小样本组：会话已撤回且小样本，双重阻断
        record = self.box.request_export(
            "EXP-AFTER", "S-WANG", self.cross_session["id"],
            [{"key": "AML-M5", "count": 3}], ["diagnosis"], self.WITHDRAW_AT)
        self.assertEqual(record["decision"], EXPORT_BLOCKED)
        self.assertTrue(any("小样本" in r for r in record["reasons"]))
        self.assertTrue(any("同意撤回" in r or "撤回" in r for r in record["reasons"]))

    def test_double_withdraw_is_rejected(self):
        self.box.withdraw_consent("CONS-AML-TEACH", self.WITHDRAW_AT)
        with self.assertRaises(SandboxError):
            self.box.withdraw_consent("CONS-AML-TEACH", self.WITHDRAW_AT)


class ErratumAndUpgradeTest(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox.from_seed(SEED)

    def test_graded_fingerprint_pins_old_versions(self):
        lin = self.box.assignments["AS-LIN-01"]
        self.assertEqual(lin["fingerprint"]["snapshot"]["version"], 1)
        self.assertEqual(lin["fingerprint"]["environment"]["version"], 1)
        types = {f["type"] for f in lin["risk_flags"]}
        self.assertEqual(types, {"病例勘误", "工具升级"})

    def test_old_snapshot_remains_immutable_after_erratum(self):
        # 勘误以新版本发布，v1 内容绝不被改写
        v1 = self.box.snapshots[("CASE-AML", 1)]
        self.assertEqual(v1["rows"][2]["cell_type"], "早幼粒细胞")
        self.assertEqual(self.box.snapshots[("CASE-AML", 2)]["rows"][2]["cell_type"],
                         "异常早幼粒细胞")

    def test_new_tasks_pin_latest_versions_only_at_creation(self):
        # 5 月新建的任务自动钉到新版本
        may_task = self.box.tasks["TASK-LOCAL-Q2"]
        self.assertEqual((may_task["snapshot_version"], may_task["environment_version"]),
                         (2, 2))
        # 但 4 月时点创建的任务只能钉到当时已发布的版本
        april_task = self.box.create_task(
            "TASK-CHECK", "C-LOCAL", "CASE-AML",
            ["diagnosis"], "POL-K5", "ENV-SCANPY", now=T("2026-04-01T00:00:00"))
        self.assertEqual((april_task["snapshot_version"], april_task["environment_version"]),
                         (1, 1))

    def test_grading_is_append_only(self):
        with self.assertRaises(SandboxError):
            self.box.grade_assignment(
                "T-CHEN", "AS-LIN-01", "C", "改分", T("2026-05-20T00:00:00"))


class TeacherReproduceTest(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox.from_seed(SEED)

    def test_teacher_reproduces_in_frozen_environment_without_identity(self):
        report = self.box.reproduce_report(
            "T-CHEN", "AS-LIN-01", T("2026-05-20T00:00:00"))
        self.assertEqual(report["status"], "复现一致")
        self.assertTrue(report["fingerprint_intact"])
        self.assertTrue(report["rerun"]["matches_graded"])
        self.assertEqual(report["identity_access"], "拒绝")
        self.assertIn("patient_id", report["identity_fields"])

    def test_teacher_cannot_reproduce_other_course(self):
        # 陈教师无权复现跨校课程的作业，即便病例相同
        with self.assertRaises(AuthorizationError):
            self.box.reproduce_report("T-CHEN", "AS-GAO-01",
                                      T("2026-05-20T00:00:00"))
        # 赵教师可以复现自己课程的作业
        report = self.box.reproduce_report(
            "T-ZHAO", "AS-GAO-01", T("2026-05-20T00:00:00"))
        self.assertEqual(report["status"], "复现一致")

    def test_teacher_capability_never_includes_identity_read(self):
        with self.assertRaises(AuthorizationError):
            self.box.read_patient_identity("T-CHEN", "CASE-AML")

    def test_repro_detects_tool_missing_in_frozen_image(self):
        # 作业使用了冻结镜像里不存在的工具 → 复现必须判为不一致
        now = T("2026-04-01T09:00:00")
        session = self.box.issue_slice("S-LIN", "TASK-LOCAL-Q1", now)
        self.box.submit_assignment(
            "AS-TAMPER", "S-LIN", "TASK-LOCAL-Q1", "可疑结论",
            [{"step": "用外部工具重聚类", "tool": "seurat"}], now,
            slice_ids=[session["id"]])
        self.box.grade_assignment(
            "T-CHEN", "AS-TAMPER", "C", "工具来源存疑", T("2026-04-05T00:00:00"))
        report = self.box.reproduce_report(
            "T-CHEN", "AS-TAMPER", T("2026-05-20T00:00:00"))
        self.assertEqual(report["status"], "复现不一致")
        self.assertEqual(report["rerun"]["missing_tools"], ["seurat"])


class ExpiryTest(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox.from_seed(SEED)
        self.session = self.box.issue_slice(
            "S-WANG", "TASK-CROSS-Q1", T("2026-05-14T09:00:00"),
            ttl_minutes=2880)

    def test_cross_institutional_course_expiry_revokes_access(self):
        revoked = self.box.sweep_expired(T("2026-05-16T00:00:00"))
        self.assertIn("S-WANG@C-CROSS", revoked)
        self.assertIn("S-GAO@C-CROSS", revoked)
        # 进行中的沙箱会话一并收回
        self.assertEqual(self.session["status"], STATUS_REVOKED)
        with self.assertRaises(AuthorizationError):
            self.box.issue_slice("S-WANG", "TASK-CROSS-Q1",
                                 T("2026-05-16T09:00:00"))

    def test_local_course_unaffected_when_cross_course_expires(self):
        self.box.sweep_expired(T("2026-05-16T00:00:00"))
        enrollment = self.box._enrollment("S-LIN", "C-LOCAL")
        self.assertNotEqual(enrollment["status"], STATUS_REVOKED)
        # 本校课程仍开放，切片正常
        session = self.box.issue_slice(
            "S-LIN", "TASK-LOCAL-Q2", T("2026-05-16T09:00:00"))
        self.assertEqual(session["status"], STATUS_SANDBOX)

    def test_sweep_is_idempotent(self):
        first = self.box.sweep_expired(T("2026-07-01T00:00:00"))
        second = self.box.sweep_expired(T("2026-07-02T00:00:00"))
        self.assertIn("S-LIN@C-LOCAL", first)
        self.assertEqual(second, [])


class TraceTest(unittest.TestCase):
    def setUp(self):
        self.box = Sandbox.from_seed(SEED)

    def test_trace_has_four_required_parts(self):
        trace = self.box.trace("AS-LIN-01")
        self.assertIn("数据范围", trace)
        self.assertIn("处理步骤", trace)
        self.assertIn("课程授权", trace)
        self.assertIn("教师复核", trace)

    def test_trace_points_to_exact_data_scope(self):
        scope = self.box.trace("AS-LIN-01")["数据范围"]
        self.assertEqual(scope["case_id"], "CASE-AML")
        self.assertEqual(scope["snapshot_version"], 1)
        self.assertEqual(scope["objective_fields"],
                         ["age", "diagnosis", "cell_type", "marker"])
        self.assertEqual(len(scope["content_hash"]), 16)

    def test_trace_records_authorization_lineage(self):
        auth = self.box.trace("AS-LIN-01")["课程授权"]
        self.assertEqual(auth["course_id"], "C-LOCAL")
        self.assertEqual(auth["student_id"], "S-LIN")
        self.assertEqual(auth["consents"][0]["consent_id"], "CONS-AML-TEACH")
        self.assertEqual(auth["consents"][0]["withdrawn_at"], None)

    def test_trace_records_teacher_review(self):
        reviews = self.box.trace("AS-LIN-01")["教师复核"]
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0]["teacher_id"], "T-CHEN")
        self.assertEqual(reviews[0]["grade"], "A")

    def test_trace_shows_withdrawal_after_it_happens(self):
        self.box.withdraw_consent("CONS-AML-TEACH", T("2026-05-05T12:00:00"))
        consent = self.box.trace("AS-LIN-01")["课程授权"]["consents"][0]
        self.assertEqual(consent["withdrawn_at"], "2026-05-05T12:00:00")
        self.assertTrue(
            any(f["type"] == "同意撤回" for f in self.box.trace("AS-LIN-01")["风险标记"]))

    def test_case_listing_covers_both_courses(self):
        listing = self.box.assignments_for_case("CASE-AML")
        self.assertEqual({row["assignment_id"] for row in listing},
                         {"AS-LIN-01", "AS-WANG-01", "AS-GAO-01"})
        self.assertEqual({row["course_id"] for row in listing},
                         {"C-LOCAL", "C-CROSS"})


def _fresh_box_with_case_t2d_v1(rows=None, released_at=T1, replaces=None,
                                erratum_note=""):
    """CASE-T2D 在种子里没有快照，是测试全新替代链的干净病例。"""
    box = Sandbox.from_seed(SEED)
    record = box.add_snapshot(
        "CASE-T2D", 1, rows if rows is not None else V1_ROWS,
        identity_fields=IDENTITY, released_at=released_at,
        replaces=replaces, erratum_note=erratum_note)
    return box, record


class SnapshotIdentityConflictTest(unittest.TestCase):
    """异内容覆盖必须被拒绝：病例编号+版本号是不可变身份。"""

    def setUp(self):
        self.box, self.v1 = _fresh_box_with_case_t2d_v1()

    def test_exact_reregistration_is_idempotent_replay(self):
        before = len(self.box.snapshots)
        again = self.box.add_snapshot(
            "CASE-T2D", 1, V1_ROWS, identity_fields=IDENTITY, released_at=T1)
        # 五要素完全一致：返回原记录，不新增版本、不记冲突
        self.assertIs(again, self.v1)
        self.assertEqual(len(self.box.snapshots), before)
        self.assertEqual(self.box.rejected_imports, [])

    def test_canonical_content_ignores_json_key_order(self):
        # 规范化（sort_keys）后内容相同：仅键顺序不同仍算精确重放
        again = self.box.add_snapshot(
            "CASE-T2D", 1, V1_ROWS_KEY_SHUFFLED,
            identity_fields=IDENTITY, released_at=T1)
        self.assertIs(again, self.v1)
        self.assertEqual(self.box.rejected_imports, [])

    def test_different_content_is_rejected_and_original_remains(self):
        with self.assertRaises(SnapshotConflict):
            self.box.add_snapshot(
                "CASE-T2D", 1, V1B_ROWS,
                identity_fields=IDENTITY, released_at=T1)
        # 原快照行数据原样保留，版本名下读不到新数据，不产生新版本
        self.assertEqual(self.box.snapshots[("CASE-T2D", 1)]["rows"], V1_ROWS)
        self.assertNotIn(("CASE-T2D", 2), self.box.snapshots)
        rejected = self.box.rejected_imports
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["category"], REJECT_CONFLICT)
        self.assertEqual(rejected[0]["version"], 1)
        self.assertEqual(rejected[0]["content_hash"], digest(V1B_ROWS))
        self.assertTrue(any(e["action"] == "快照导入拒绝" for e in self.box.audit))

    def test_each_identity_part_change_is_a_conflict(self):
        # 身份字段变化
        with self.assertRaises(SnapshotConflict):
            self.box.add_snapshot("CASE-T2D", 1, V1_ROWS,
                                  identity_fields=["patient_id", "phone"],
                                  released_at=T1)
        # 发布时间变化
        with self.assertRaises(SnapshotConflict):
            self.box.add_snapshot("CASE-T2D", 1, V1_ROWS,
                                  identity_fields=IDENTITY,
                                  released_at=T("2026-02-21T00:00:00"))
        # 勘误说明变化
        with self.assertRaises(SnapshotConflict):
            self.box.add_snapshot("CASE-T2D", 1, V1_ROWS,
                                  identity_fields=IDENTITY, released_at=T1,
                                  erratum_note="事后补的说明")
        # 替代关系变化：先合法登记 v2 replaces=1，再以 replaces=None 重传 v2
        self.box.add_snapshot("CASE-T2D", 2, V1B_ROWS,
                              identity_fields=IDENTITY, released_at=T2,
                              replaces=1, erratum_note="更正年龄")
        with self.assertRaises(SnapshotConflict):
            self.box.add_snapshot("CASE-T2D", 2, V1B_ROWS,
                                  identity_fields=IDENTITY, released_at=T2)
        self.assertEqual(self.box.snapshots[("CASE-T2D", 1)]["rows"], V1_ROWS)
        self.assertEqual(
            {entry["category"] for entry in self.box.rejected_imports},
            {REJECT_CONFLICT})

    def test_conflict_keeps_task_slice_export_and_graded_work_intact(self):
        box = Sandbox.from_seed(SEED)
        now = T("2026-04-01T09:00:00")
        session = box.issue_slice("S-LIN", "TASK-LOCAL-Q1", now)
        export = box.request_export(
            "EXP-KEEP", "S-LIN", session["id"],
            [{"key": "AML-M2", "count": 5}], ["diagnosis"], now,
            assignment_id="AS-LIN-01")
        self.assertEqual(export["decision"], EXPORT_APPROVED)
        original_rows = box.snapshots[("CASE-AML", 1)]["rows"]
        assignment = box.assignments["AS-LIN-01"]
        frozen_fp = json.dumps(assignment["fingerprint"], sort_keys=True,
                               ensure_ascii=False, default=str)

        # 数据管理员误用已发布版本号 v1 重传另一批记录
        tampered = [dict(row, marker="CD99+") for row in original_rows]
        with self.assertRaises(SnapshotConflict):
            box.add_snapshot("CASE-AML", 1, tampered,
                             identity_fields=["patient_id", "patient_name", "phone"],
                             released_at=T("2026-02-20T00:00:00"))

        # 快照、任务钉版、已签发切片、导出、已评分作业全部不变
        self.assertEqual(box.snapshots[("CASE-AML", 1)]["rows"], original_rows)
        self.assertEqual(box.tasks["TASK-LOCAL-Q1"]["snapshot_version"], 1)
        self.assertEqual(session["rows"][0]["marker"], "CD34+")
        self.assertEqual(box.exports["EXP-KEEP"]["decision"], EXPORT_APPROVED)
        self.assertEqual(assignment["status"], STATUS_GRADED)
        self.assertEqual(assignment["grade"], "A")
        self.assertEqual(
            json.dumps(assignment["fingerprint"], sort_keys=True,
                       ensure_ascii=False, default=str),
            frozen_fp)
        report = box.reproduce_report("T-CHEN", "AS-LIN-01",
                                      T("2026-05-20T00:00:00"))
        self.assertEqual(report["status"], "复现一致")
        self.assertTrue(report["fingerprint_intact"])


class ReplacementChainTest(unittest.TestCase):
    """乱序、跳版、分叉、循环、时间倒退的替代关系一律不得入库。"""

    def setUp(self):
        self.box, _ = _fresh_box_with_case_t2d_v1()

    def test_out_of_order_replacement_is_rejected(self):
        # v2 声明替代尚不存在的 v1 之前：直接登记 v2 replaces=1（无 v1 时 v2 也非法）
        box = Sandbox.from_seed(SEED)
        with self.assertRaises(SandboxError):
            box.add_snapshot("CASE-T2D", 2, V1B_ROWS,
                             identity_fields=IDENTITY, released_at=T2, replaces=1)
        self.assertNotIn(("CASE-T2D", 2), box.snapshots)
        self.assertEqual(len(box.snapshots), 2)  # 种子 CASE-AML 的两版

    def test_skip_version_is_rejected(self):
        with self.assertRaises(SandboxError):
            self.box.add_snapshot("CASE-T2D", 3, V1B_ROWS,
                                  identity_fields=IDENTITY,
                                  released_at=T3, replaces=1)
        self.assertNotIn(("CASE-T2D", 3), self.box.snapshots)

    def test_forked_replacement_is_rejected(self):
        self.box.add_snapshot("CASE-T2D", 2, V1B_ROWS,
                              identity_fields=IDENTITY, released_at=T2,
                              replaces=1, erratum_note="v2")
        # 另起一条分支再次替代 v1：链不允许分叉
        with self.assertRaises(SandboxError):
            self.box.add_snapshot("CASE-T2D", 3, V1B_ROWS,
                                  identity_fields=IDENTITY,
                                  released_at=T3, replaces=1)
        self.assertNotIn(("CASE-T2D", 3), self.box.snapshots)

    def test_released_time_regression_is_rejected(self):
        with self.assertRaises(SandboxError):
            self.box.add_snapshot("CASE-T2D", 2, V1B_ROWS,
                                  identity_fields=IDENTITY,
                                  released_at=T("2026-01-01T00:00:00"),
                                  replaces=1)

    def test_self_or_backward_replacement_is_rejected_as_cycle(self):
        for bad_replaces in (1, 2):
            with self.assertRaises(SandboxError):
                self.box.add_snapshot("CASE-T2D", 1, V1B_ROWS,
                                      identity_fields=IDENTITY,
                                      released_at=T1, replaces=bad_replaces)

    def test_first_version_must_be_v1_and_second_must_declare_replaces(self):
        box = Sandbox.from_seed(SEED)
        with self.assertRaises(SandboxError):
            box.add_snapshot("CASE-T2D", 2, V1_ROWS,
                             identity_fields=IDENTITY, released_at=T1)
        box.add_snapshot("CASE-T2D", 1, V1_ROWS,
                         identity_fields=IDENTITY, released_at=T1)
        with self.assertRaises(SandboxError):
            box.add_snapshot("CASE-T2D", 2, V1B_ROWS,
                             identity_fields=IDENTITY, released_at=T2)

    def test_legal_sequential_chain_is_stored(self):
        self.box.add_snapshot("CASE-T2D", 2, V1B_ROWS,
                              identity_fields=IDENTITY, released_at=T2,
                              replaces=1, erratum_note="年龄更正")
        self.box.add_snapshot("CASE-T2D", 3, V1B_ROWS,
                              identity_fields=IDENTITY, released_at=T3,
                              replaces=2, erratum_note="再次复核")
        lineage = self.box.snapshot_lineage("CASE-T2D")
        self.assertEqual([c["version"] for c in lineage["chain"]], [1, 2, 3])
        self.assertEqual(lineage["current_version"], 3)
        self.assertEqual(lineage["head_versions"], [3])


class ConcurrentSubmissionTest(unittest.TestCase):
    """并发导入同一版本：只能有一个首次提交者。"""

    def test_identical_concurrent_submissions_share_one_record(self):
        box = Sandbox.from_seed(SEED)
        barrier = threading.Barrier(8)

        def submit():
            barrier.wait()
            return box.add_snapshot("CASE-T2D", 1, V1_ROWS,
                                    identity_fields=IDENTITY, released_at=T1)

        with ThreadPoolExecutor(max_workers=8) as pool:
            records = list(pool.map(lambda _: submit(), range(8)))
        # 一人首次提交，其余全部幂等重放到同一条记录
        self.assertEqual(len({id(r) for r in records}), 1)
        self.assertEqual(len(box.snapshots), 3)
        self.assertEqual(box.rejected_imports, [])

    def test_all_distinct_concurrent_payloads_have_exactly_one_winner(self):
        # 12 个线程各持不同 rows 抢 v1：恰有一个首次提交者，其余全部冲突
        box = Sandbox.from_seed(SEED)
        n = 12
        barrier = threading.Barrier(n)
        outcomes = []
        lock = threading.Lock()
        distinct_rows = [[{"patient_id": f"P-{i}", "age": 20 + i}] for i in range(n)]

        def submit(i):
            barrier.wait()
            try:
                box.add_snapshot("CASE-T2D", 1, distinct_rows[i],
                                 identity_fields=IDENTITY, released_at=T1)
                with lock:
                    outcomes.append(("ok", i))
            except SnapshotConflict:
                with lock:
                    outcomes.append(("conflict", i))

        with ThreadPoolExecutor(max_workers=n) as pool:
            list(pool.map(submit, range(n)))
        winners = [i for kind, i in outcomes if kind == "ok"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(outcomes) - 1, n - 1)
        # 入库内容必须就是获胜线程提交的那份
        self.assertEqual(box.snapshots[("CASE-T2D", 1)]["rows"],
                         distinct_rows[winners[0]])
        self.assertEqual(len(box.rejected_imports), n - 1)

    def test_distinct_concurrent_payloads_have_single_winner(self):
        # 5/5 两种相异载荷同时抢同一身份：无论哪种载荷胜出，
        # 都只能有一次真正的首次插入，持有失败载荷的 5 个线程全部冲突
        for _ in range(20):  # 重复多轮以暴露潜在的 check-then-insert 竞态
            box = Sandbox.from_seed(SEED)
            barrier = threading.Barrier(10)
            outcomes = []
            lock = threading.Lock()

            def submit(use_b):
                barrier.wait()
                rows = V1B_ROWS if use_b else V1_ROWS
                try:
                    box.add_snapshot("CASE-T2D", 1, rows,
                                     identity_fields=IDENTITY, released_at=T1)
                    with lock:
                        outcomes.append("ok")
                except SnapshotConflict:
                    with lock:
                        outcomes.append("conflict")

            with ThreadPoolExecutor(max_workers=10) as pool:
                list(pool.map(submit, [i % 2 == 0 for i in range(10)]))
            self.assertEqual(outcomes.count("ok"), 5)
            self.assertEqual(outcomes.count("conflict"), 5)
            # 只有一个首次提交者：CASE-T2D 只有 v1 一版，登记审计仅一条
            self.assertEqual(
                [k for k in box.snapshots if k[0] == "CASE-T2D"],
                [("CASE-T2D", 1)])
            inserts = [e for e in box.audit
                       if e.get("action") == "快照登记"
                       and e.get("case_id") == "CASE-T2D"]
            self.assertEqual(len(inserts), 1)
            self.assertEqual(len(box.rejected_imports), 5)
            self.assertTrue(
                all(e["category"] == REJECT_CONFLICT for e in box.rejected_imports))


class JournalRecoveryTest(unittest.TestCase):
    """服务重启后的日志恢复执行与在线导入相同的校验。"""

    def _write(self, name, content):
        path = self.tmp / name
        path.write_text(content, encoding="utf-8")
        return path

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.box = Sandbox.from_seed(SEED)

    def test_journal_recovery_isolates_bad_records_without_aborting(self):
        v1 = {"case_id": "CASE-T2D", "version": 1, "rows": V1_ROWS,
              "identity_fields": IDENTITY, "released_at": T1.isoformat(),
              "content_hash": digest(V1_ROWS)}
        v2 = {"case_id": "CASE-T2D", "version": 2, "rows": V1B_ROWS,
              "identity_fields": IDENTITY, "released_at": T2.isoformat(),
              "replaces": 1, "erratum_note": "年龄更正",
              "content_hash": digest(V1B_ROWS)}
        bad_hash = dict(v2, version=3, replaces=2,
                        released_at=T3.isoformat(), content_hash="0" * 16)
        gap = dict(v2, version=5, replaces=4, released_at=T3.isoformat())
        conflict = dict(v1, rows=V1B_ROWS, content_hash=digest(V1B_ROWS))
        lines = [
            json.dumps(v1, ensure_ascii=False),
            json.dumps(v2, ensure_ascii=False),
            '{"case_id": "CASE-T2D", "version": 9, "rows": [损坏',
            json.dumps(bad_hash, ensure_ascii=False),
            json.dumps(gap, ensure_ascii=False),
            json.dumps(v1, ensure_ascii=False),           # 精确重放
            json.dumps(conflict, ensure_ascii=False),     # 异内容覆盖
        ]
        path = self._write("snapshots.jsonl", "\n".join(lines))
        stats = self.box.replay_snapshot_journal(path)
        self.assertEqual(stats,
                         {"accepted": 2, "replayed": 1, "rejected": 4})
        # 只有合法的 v1/v2 进入分账，且内容与日志一致
        self.assertEqual(
            set(self.box.snapshots),
            {("CASE-AML", 1), ("CASE-AML", 2), ("CASE-T2D", 1),
             ("CASE-T2D", 2)})
        self.assertEqual(self.box.snapshots[("CASE-T2D", 1)]["rows"], V1_ROWS)
        categories = {e["category"] for e in self.box.rejected_imports}
        self.assertEqual(categories,
                         {REJECT_CORRUPT, REJECT_CHECKSUM,
                          REJECT_REPLACEMENT, REJECT_CONFLICT})
        # 每条拒绝都能定位到日志文件与行号
        for entry in self.box.rejected_imports:
            self.assertTrue(entry["source"].startswith(str(path)))
            self.assertTrue(entry["source"].rsplit(":", 1)[-1].isdigit())
        # 拒绝记录顺序稳定、只追加
        self.assertEqual([e["seq"] for e in self.box.rejected_imports],
                         [1, 2, 3, 4])

    def test_reloading_same_journal_is_idempotent(self):
        v1 = {"case_id": "CASE-T2D", "version": 1, "rows": V1_ROWS,
              "identity_fields": IDENTITY, "released_at": T1.isoformat()}
        path = self._write("twice.jsonl", json.dumps(v1, ensure_ascii=False))
        first = Sandbox.from_seed(SEED)
        stats1 = first.replay_snapshot_journal(path)
        stats2 = first.replay_snapshot_journal(path)
        self.assertEqual(stats1, {"accepted": 1, "replayed": 0, "rejected": 0})
        self.assertEqual(stats2, {"accepted": 0, "replayed": 1, "rejected": 0})
        # 全新进程重建后状态一致
        rebuilt = Sandbox.from_seed(SEED)
        rebuilt.replay_snapshot_journal(path)
        self.assertEqual(
            rebuilt.snapshots[("CASE-T2D", 1)]["content_hash"],
            first.snapshots[("CASE-T2D", 1)]["content_hash"])

    def test_envelope_json_and_single_object_formats(self):
        v1 = {"case_id": "CASE-T2D", "version": 1, "rows": V1_ROWS,
              "identity_fields": IDENTITY, "released_at": T1.isoformat()}
        envelope = self._write("envelope.json",
                               json.dumps({"snapshots": [v1]}, ensure_ascii=False))
        box = Sandbox.from_seed(SEED)
        self.assertEqual(box.replay_snapshot_journal(envelope)["accepted"], 1)
        single = self._write("single.json", json.dumps(v1, ensure_ascii=False))
        box2 = Sandbox.from_seed(SEED)
        self.assertEqual(box2.replay_snapshot_journal(single)["accepted"], 1)

    def test_recovered_snapshots_pin_new_tasks_and_project_slices(self):
        v1 = {"case_id": "CASE-T2D", "version": 1, "rows": V1_ROWS,
              "identity_fields": IDENTITY, "released_at": T1.isoformat()}
        v2 = {"case_id": "CASE-T2D", "version": 2, "rows": V1B_ROWS,
              "identity_fields": IDENTITY, "released_at": T2.isoformat(),
              "replaces": 1, "erratum_note": "年龄更正"}
        path = self._write("chain.jsonl", "\n".join(
            json.dumps(x, ensure_ascii=False) for x in (v1, v2)))
        self.box.replay_snapshot_journal(path)
        # 恢复后新建任务钉到当前有效版本 v2
        self.box.grant_consent(
            "CONS-T2D", "CASE-T2D", course_id=None,
            granted_at=T("2026-02-25T00:00:00"))
        task = self.box.create_task(
            "TASK-T2D", "C-LOCAL", "CASE-T2D", ["age", "diagnosis"],
            "POL-K5", "ENV-SCANPY", now=T("2026-05-20T00:00:00"))
        self.assertEqual(task["snapshot_version"], 2)

    def test_tampered_replay_does_not_disturb_frozen_assignment(self):
        original = Sandbox.from_seed(SEED).snapshots[("CASE-AML", 1)]
        fake = {"case_id": "CASE-AML", "version": 1,
                "rows": [{"patient_id": "X", "age": 99}],
                "identity_fields": original["identity_fields"],
                "released_at": original["released_at"].isoformat(),
                "content_hash": digest([{"patient_id": "X", "age": 99}]),
                "erratum_note": "恶意覆盖"}
        path = self._write("tamper.jsonl", json.dumps(fake, ensure_ascii=False))
        stats = self.box.replay_snapshot_journal(path)
        self.assertEqual(stats["rejected"], 1)
        self.assertEqual(self.box.rejected_imports[0]["category"], REJECT_CONFLICT)
        report = self.box.reproduce_report(
            "T-CHEN", "AS-LIN-01", T("2026-05-20T00:00:00"))
        self.assertEqual(report["status"], "复现一致")
        self.assertTrue(report["fingerprint_intact"])


class SeedLoadValidationTest(unittest.TestCase):
    """夹具装载（重启路径）本身也走同一套登记校验。"""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def _seed_with(self, snapshots):
        data = json.loads(Path(SEED).read_text(encoding="utf-8"))
        data["snapshots"] = snapshots
        path = self.tmp / "seed.json"
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        return path

    def test_seed_with_conflicting_duplicate_version_fails_to_load(self):
        data = json.loads(Path(SEED).read_text(encoding="utf-8"))
        evil_v1 = dict(data["snapshots"][0])
        evil_v1["rows"] = [{"patient_id": "P-X", "age": 99}]
        data["snapshots"].insert(1, evil_v1)
        path = self.tmp / "dup.json"
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        with self.assertRaises(SnapshotConflict):
            Sandbox.from_seed(path)

    def test_seed_with_out_of_order_replacement_fails_to_load(self):
        data = json.loads(Path(SEED).read_text(encoding="utf-8"))
        data["snapshots"].append({
            "case_id": "CASE-AML", "version": 9,
            "rows": data["snapshots"][0]["rows"],
            "identity_fields": ["patient_id"],
            "released_at": "2026-07-01T00:00:00", "replaces": 8})
        path = self.tmp / "gap.json"
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        with self.assertRaises(SandboxError):
            Sandbox.from_seed(path)


class SubsequentErratumAndLineageTest(unittest.TestCase):
    """合法新版本沿替代链追加风险标记；追溯视图同时呈现冻结/当前/拒绝。"""

    def setUp(self):
        self.box = Sandbox.from_seed(SEED)
        self.v3 = self.box.add_snapshot(
            "CASE-AML", 3,
            self.box.snapshots[("CASE-AML", 2)]["rows"],
            identity_fields=["patient_id", "patient_name", "phone"],
            released_at=T3, replaces=2,
            erratum_note="复核确认 APL 疑点并补注采样批次")

    def test_v3_flags_graded_assignments_anywhere_on_chain(self):
        # AS-LIN-01 与 AS-GAO-01 都钉在链上的 v1，均应收到 v3 勘误标记
        for assignment_id in ("AS-LIN-01", "AS-GAO-01"):
            flags = self.box.assignments[assignment_id]["risk_flags"]
            erratum_versions = {f["current_version"] for f in flags
                                if f["type"] == "病例勘误"}
            self.assertIn(3, erratum_versions)
        lin = self.box.assignments["AS-LIN-01"]
        self.assertEqual(lin["fingerprint"]["snapshot"]["version"], 1)

    def test_exact_replay_adds_no_duplicate_flags(self):
        before = [dict(f) for f in
                  self.box.assignments["AS-LIN-01"]["risk_flags"]]
        again = self.box.add_snapshot(
            "CASE-AML", 3, self.v3["rows"],
            identity_fields=self.v3["identity_fields"],
            released_at=T3, replaces=2,
            erratum_note="复核确认 APL 疑点并补注采样批次")
        self.assertIs(again, self.v3)
        after = self.box.assignments["AS-LIN-01"]["risk_flags"]
        self.assertEqual(before, after)

    def test_conflicting_v3_reupload_changes_nothing(self):
        flags_before = [dict(f) for f in
                        self.box.assignments["AS-LIN-01"]["risk_flags"]]
        with self.assertRaises(SnapshotConflict):
            self.box.add_snapshot(
                "CASE-AML", 3, self.v3["rows"],
                identity_fields=self.v3["identity_fields"],
                released_at=T3, replaces=2, erratum_note="被篡改的说明")
        self.assertEqual(
            self.box.assignments["AS-LIN-01"]["risk_flags"], flags_before)
        self.assertEqual(self.box.snapshots[("CASE-AML", 3)]["erratum_note"],
                         "复核确认 APL 疑点并补注采样批次")

    def test_trace_shows_frozen_current_and_rejected(self):
        with self.assertRaises(SnapshotConflict):
            self.box.add_snapshot(
                "CASE-AML", 1, [{"patient_id": "P-X"}],
                identity_fields=["patient_id"],
                released_at=T("2026-02-20T00:00:00"))
        lineage = self.box.trace("AS-LIN-01")["版本谱系"]
        self.assertEqual(lineage["frozen_version"], 1)
        self.assertFalse(lineage["frozen_is_current"])
        self.assertEqual(lineage["current_version"], 3)
        self.assertEqual([c["version"] for c in lineage["chain"]], [1, 2, 3])
        self.assertTrue(all("content_hash" in c for c in lineage["chain"]))
        self.assertEqual(
            {e["category"] for e in lineage["rejected_imports"]},
            {REJECT_CONFLICT})
        self.assertTrue(all(e["version"] == 1 for e in lineage["rejected_imports"]))

    def test_lineage_reports_current_version(self):
        lineage = self.box.snapshot_lineage("CASE-AML")
        self.assertEqual(lineage["current_version"], 3)
        self.assertEqual(lineage["head_versions"], [3])
        chain = {c["version"]: c for c in lineage["chain"]}
        self.assertEqual(chain[1]["replaces"], None)
        self.assertEqual(chain[2]["replaces"], 1)
        self.assertEqual(chain[3]["replaces"], 2)
        self.assertTrue(chain[3]["is_current"])
        self.assertFalse(chain[1]["is_current"])


if __name__ == "__main__":
    unittest.main()
