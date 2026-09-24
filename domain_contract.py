"""医学教学数据沙箱的领域契约测试。

覆盖需求中的全部治理承诺：
限时切片与脱敏、披露检查与小样本阻断、跨校到期收权、
勘误/升级前向影响、同意撤回即时效力、已评分作业指纹冻结、
教师复现与身份隔离、结论四要素溯源。
"""

import copy
import json
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path

from domain import (
    EXPORT_APPROVED,
    EXPORT_BLOCKED,
    REGISTER_FIRST,
    REJECT_CONTENT,
    REJECT_CORRUPT,
    REJECT_REPLACES_CYCLE,
    REJECT_REPLACES_MISSING,
    REJECT_REPLACES_ORDER,
    STATUS_GRADED,
    STATUS_REVOKED,
    STATUS_SANDBOX,
    AuthorizationError,
    Sandbox,
    SandboxError,
    SnapshotRegistrationError,
    digest,
)

SEED = "fixtures/seed.json"
T = lambda s: datetime.fromisoformat(s)  # noqa: E731


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


class SnapshotIdentityTest(unittest.TestCase):
    """(病例, 版本) 不可变身份：异内容必须冲突，精确重放必须幂等。"""

    def setUp(self):
        self.box = Sandbox.from_seed(SEED)
        self.seed_data = json.loads(Path(SEED).read_text(encoding="utf-8"))
        self.v1 = self.seed_data["snapshots"][0]
        self.v2 = self.seed_data["snapshots"][1]

    def _resubmit_v1(self, **overrides):
        params = {
            "case_id": "CASE-AML", "version": 1, "rows": self.v1["rows"],
            "identity_fields": self.v1["identity_fields"],
            "released_at": T(self.v1["released_at"]),
            "replaces": self.v1.get("replaces"),
            "erratum_note": self.v1.get("erratum_note", ""),
            "submitted_by": "重传管理员",
        }
        params.update(overrides)
        return self.box.add_snapshot(**params)

    def test_different_content_same_version_is_rejected_not_overwritten(self):
        original = self.box.snapshots[("CASE-AML", 1)]
        bad_rows = copy.deepcopy(self.v1["rows"])
        bad_rows[2]["cell_type"] = "被篡改的注释"
        with self.assertRaises(SnapshotRegistrationError) as caught:
            self._resubmit_v1(rows=bad_rows)
        self.assertEqual(caught.exception.reason, REJECT_CONTENT)
        self.assertEqual(caught.exception.diff, ["content_hash"])
        # 原快照对象与内容原样保留
        self.assertIs(self.box.snapshots[("CASE-AML", 1)], original)
        self.assertEqual(original["rows"][2]["cell_type"], "早幼粒细胞")
        self.assertEqual(len(self.box.snapshots), 2)

    def test_each_identity_part_change_is_a_conflict(self):
        # 身份字段、发布时间、替代关系、勘误说明任一部分变化都必须记录冲突
        cases = [
            {"identity_fields": ["patient_id"]},
            {"released_at": T("2026-03-01T00:00:00")},
            {"replaces": 2},
            {"erratum_note": "偷偷补一句勘误"},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(SnapshotRegistrationError) as caught:
                    self._resubmit_v1(**overrides)
                self.assertEqual(caught.exception.reason, REJECT_CONTENT)
                self.assertEqual(len(caught.exception.diff), 1)
        self.assertEqual(len(self.box.rejected_imports), len(cases))

    def test_conflict_leaves_tasks_slices_exports_and_grades_untouched(self):
        now = T("2026-04-01T09:00:00")
        session = self.box.issue_slice("S-LIN", "TASK-LOCAL-Q1", now)
        export = self.box.request_export(
            "EXP-KEEP", "S-LIN", session["id"],
            [{"key": "AML-M2", "count": 5}], ["diagnosis"], now,
            assignment_id="AS-LIN-01")
        assignment = self.box.assignments["AS-LIN-01"]
        frozen_fp = copy.deepcopy(assignment["fingerprint"])

        bad_rows = copy.deepcopy(self.v1["rows"])
        bad_rows[0]["age"] = 99
        with self.assertRaises(SnapshotRegistrationError):
            self._resubmit_v1(rows=bad_rows)

        # 已签发切片仍是冻结的旧数据（42 岁按十岁段泛化为 40-49）
        self.assertEqual(session["rows"][0]["age"], "40-49")
        # 重新签发的切片也读不到“新数据”
        again = self.box.issue_slice("S-LIN", "TASK-LOCAL-Q1", now)
        self.assertEqual(again["rows"][2]["cell_type"], "早幼粒细胞")
        # 导出决策、已评分作业与冻结指纹不变
        self.assertEqual(export["decision"], EXPORT_APPROVED)
        self.assertEqual(assignment["fingerprint"], frozen_fp)
        self.assertEqual(assignment["grade"], "A")
        self.assertEqual(self.box.tasks["TASK-LOCAL-Q1"]["snapshot_version"], 1)

    def test_exact_replay_is_idempotent_and_keeps_first_submitter(self):
        before = len(self.box.audit)
        returned = self._resubmit_v1(submitted_by="另一位管理员")
        self.assertIs(returned, self.box.snapshots[("CASE-AML", 1)])
        self.assertEqual(returned["register_status"], REGISTER_FIRST)
        self.assertEqual(returned["registered_by"], "种子装载")
        self.assertEqual(self.box.rejected_imports, [])
        self.assertEqual(len(self.box.snapshots), 2)
        self.assertTrue(any(row["action"] == "快照精确重放"
                            for row in self.box.audit[before:]))
        # 再重放一次仍然幂等
        self._resubmit_v1(submitted_by="第三位管理员")
        self.assertEqual(len(self.box.snapshots), 2)

    def test_rejected_import_is_listed_in_lineage_and_trace(self):
        bad_rows = copy.deepcopy(self.v1["rows"])
        bad_rows[0]["diagnosis"] = "AML-M7"
        with self.assertRaises(SnapshotRegistrationError):
            self._resubmit_v1(rows=bad_rows)
        lineage = self.box.case_lineage("CASE-AML")
        self.assertEqual(lineage["frozen"], None)
        self.assertEqual(lineage["current_version"], 2)
        self.assertEqual(len(lineage["rejected_imports"]), 1)
        self.assertEqual(lineage["rejected_imports"][0]["version"], 1)
        self.assertEqual(lineage["rejected_imports"][0]["diff"], ["content_hash"])

        trace_lineage = self.box.trace("AS-LIN-01")["快照谱系"]
        self.assertEqual(trace_lineage["frozen"]["version"], 1)
        self.assertEqual(trace_lineage["frozen"]["content_hash"],
                         self.box.snapshots[("CASE-AML", 1)]["content_hash"])
        self.assertTrue(trace_lineage["superseded"])
        self.assertEqual(trace_lineage["current_version"], 2)
        self.assertEqual(len(trace_lineage["rejected_imports"]), 1)


class ConcurrentRegistrationTest(unittest.TestCase):
    """并发导入同一版本：只能有一个首次提交者。"""

    def setUp(self):
        self.box = Sandbox.from_seed(SEED)
        self.v2_rows = json.loads(Path(SEED).read_text(encoding="utf-8"))[
            "snapshots"][1]["rows"]
        self.released = T("2026-06-10T00:00:00")

    def _concurrent(self, rows_by_team):
        barrier = threading.Barrier(sum(len(v) for v in rows_by_team.values()))
        results = {}

        def worker(team, index, rows):
            barrier.wait()
            try:
                self.box.add_snapshot(
                    "CASE-AML", 3, rows,
                    ["patient_id", "patient_name", "phone"],
                    self.released, replaces=2,
                    erratum_note="v3 联调勘误",
                    submitted_by=f"{team}-{index}",
                    submitted_at=self.released)
                results[(team, index)] = ("ok", None)
            except SnapshotRegistrationError as exc:
                results[(team, index)] = ("reject", exc.reason)

        threads = []
        for team, rows_list in rows_by_team.items():
            for index, rows in enumerate(rows_list):
                threads.append(threading.Thread(
                    target=worker, args=(team, index, rows)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return results

    def test_only_one_first_submitter_under_identical_concurrency(self):
        rows_list = [copy.deepcopy(self.v2_rows) for _ in range(6)]
        results = self._concurrent({"ADMIN": rows_list})
        self.assertEqual({outcome for outcome, _ in results.values()}, {"ok"})
        record = self.box.snapshots[("CASE-AML", 3)]
        submitters = {record["registered_by"]}
        self.assertEqual(len(submitters), 1)
        firsts = [row for row in self.box.audit
                  if row["action"] == "快照首次登记"
                  and row["case_id"] == "CASE-AML" and row["version"] == 3]
        self.assertEqual(len(firsts), 1)

    def test_divergent_concurrent_imports_leave_one_winner_and_rest_rejected(self):
        rows_a = copy.deepcopy(self.v2_rows)
        rows_b = copy.deepcopy(self.v2_rows)
        rows_b[0]["age"] = 77
        results = self._concurrent({
            "A": [copy.deepcopy(rows_a) for _ in range(3)],
            "B": [copy.deepcopy(rows_b) for _ in range(3)],
        })
        ok = [k for k, v in results.items() if v[0] == "ok"]
        rejected = [v for v in results.values() if v[0] == "reject"]
        self.assertEqual(len(ok), 3)        # 1 个首次提交者 + 2 个幂等重放
        self.assertEqual(len(rejected), 3)  # 异内容组全部冲突
        self.assertTrue(all(reason == REJECT_CONTENT for _, reason in rejected))
        # 获胜组三人的结果必须来自同一组（要么全 A，要么全 B）
        self.assertEqual(len({k[0] for k in ok}), 1)
        winner = ok[0][0]
        stored = self.box.snapshots[("CASE-AML", 3)]
        self.assertEqual(stored["content_hash"],
                         digest(rows_a if winner == "A" else rows_b))
        self.assertEqual(len(self.box.rejected_imports), 3)


class ReplacementChainTest(unittest.TestCase):
    """乱序、缺失、成环替代不得进入存储；合法新版本沿替代链标记作业。"""

    def setUp(self):
        self.box = Sandbox.from_seed(SEED)
        self.now = T("2026-06-10T00:00:00")
        self.v3_rows = copy.deepcopy(json.loads(
            Path(SEED).read_text(encoding="utf-8"))["snapshots"][1]["rows"])
        self.v3_rows[0]["marker"] = "CD34+/CD38-"

    def _add_v3(self, **overrides):
        params = {
            "case_id": "CASE-AML", "version": 3, "rows": self.v3_rows,
            "identity_fields": ["patient_id", "patient_name", "phone"],
            "released_at": self.now, "replaces": 2,
            "erratum_note": "P-001 标记补充 CD38 阴性",
        }
        params.update(overrides)
        return self.box.add_snapshot(**params)

    def test_out_of_order_and_missing_replacement_are_rejected(self):
        with self.assertRaises(SnapshotRegistrationError) as caught:
            self._add_v3(version=5, replaces=4)
        self.assertEqual(caught.exception.reason, REJECT_REPLACES_MISSING)
        with self.assertRaises(SnapshotRegistrationError) as caught:
            self._add_v3(version=0, replaces=1)
        self.assertEqual(caught.exception.reason, REJECT_REPLACES_ORDER)
        with self.assertRaises(SnapshotRegistrationError) as caught:
            self._add_v3(replaces=3)
        self.assertEqual(caught.exception.reason, REJECT_REPLACES_CYCLE)
        self.assertNotIn(("CASE-AML", 5), self.box.snapshots)
        self.assertNotIn(("CASE-AML", 0), self.box.snapshots)

    def test_cycle_in_corrupted_storage_is_rejected_on_new_registration(self):
        # 模拟外部存储已损坏：v1/v2 互相替代
        self.box.add_case("CASE-X", "坏链病例")
        base = {"case_id": "CASE-X", "rows": [], "identity_fields": [],
                "erratum_note": "", "content_hash": digest([])}
        self.box.snapshots[("CASE-X", 1)] = {
            **base, "version": 1, "replaces": 2,
            "released_at": T("2026-01-01T00:00:00")}
        self.box.snapshots[("CASE-X", 2)] = {
            **base, "version": 2, "replaces": 1,
            "released_at": T("2026-02-01T00:00:00")}
        with self.assertRaises(SnapshotRegistrationError) as caught:
            self.box.add_snapshot(
                "CASE-X", 3, [], [], T("2026-03-01T00:00:00"), replaces=1)
        self.assertEqual(caught.exception.reason, REJECT_REPLACES_CYCLE)

    def test_legal_new_version_flags_assignments_along_full_chain(self):
        # 先在 v2 上补一份已评分作业
        issued = T("2026-05-12T09:00:00")
        session = self.box.issue_slice("S-LIN", "TASK-LOCAL-Q2", issued)
        self.box.submit_assignment(
            "AS-LIN-02", "S-LIN", "TASK-LOCAL-Q2", "基于 v2 的结论",
            [{"step": "聚类", "tool": "scanpy"}], issued, slice_ids=[session["id"]])
        self.box.grade_assignment(
            "T-CHEN", "AS-LIN-02", "A", "通过", T("2026-05-20T00:00:00"))

        record = self._add_v3()
        self.assertEqual(record["register_status"], REGISTER_FIRST)
        # 基于 v1 的作业：v3 沿 v2→v1 链也要被标记
        for assignment_id, pinned in (("AS-LIN-01", 1), ("AS-GAO-01", 1),
                                      ("AS-LIN-02", 2)):
            flags = [f for f in self.box.assignments[assignment_id]["risk_flags"]
                     if f["type"] == "病例勘误" and f["current_version"] == 3]
            self.assertEqual(len(flags), 1, f"{assignment_id} 应被 v3 标记")
        # 精确重放 v3 不产生重复标记
        self._add_v3()
        for assignment_id in ("AS-LIN-01", "AS-GAO-01", "AS-LIN-02"):
            flags = [f for f in self.box.assignments[assignment_id]["risk_flags"]
                     if f["type"] == "病例勘误" and f["current_version"] == 3]
            self.assertEqual(len(flags), 1)
        # 旧任务不回溯，新任务钉到 v3
        self.assertEqual(self.box.tasks["TASK-LOCAL-Q1"]["snapshot_version"], 1)
        newest = self.box.create_task(
            "TASK-AFTER-V3", "C-LOCAL", "CASE-AML", ["diagnosis"], "POL-K5",
            "ENV-SCANPY", now=T("2026-06-11T00:00:00"))
        self.assertEqual(newest["snapshot_version"], 3)


class SnapshotRecoveryOnLoadTest(unittest.TestCase):
    """服务重启后的装载执行同一校验：坏快照隔离，其余账目恢复。"""

    def _load_dict(self, data):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                         encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False)
            path = handle.name
        return Sandbox.from_seed(path), path

    def test_bad_snapshot_in_fixture_is_quarantined_without_blocking_reload(self):
        data = json.loads(Path(SEED).read_text(encoding="utf-8"))
        data["snapshots"].append({
            "case_id": "CASE-AML", "version": 9,
            "released_at": "2026-07-01T00:00:00", "replaces": 8,
            "identity_fields": ["patient_id"], "rows": [],
        })
        box, _path = self._load_dict(data)
        reasons = {row["reason"] for row in box.rejected_imports}
        self.assertIn(REJECT_REPLACES_MISSING, reasons)
        self.assertEqual(len(box.snapshots), 2)  # v1/v2 原样恢复
        self.assertEqual(len(box.assignments), 3)
        self.assertTrue(any(
            w.get("version") == 9 for w in box.load_warnings))

    def test_corrupt_published_version_quarantines_dependent_records(self):
        data = json.loads(Path(SEED).read_text(encoding="utf-8"))
        # 账本中固化的登记摘要仍正确，但 v1 行数据已被外部篡改（异内容覆盖）
        original_hash = digest(data["snapshots"][0]["rows"])
        for entry in data["snapshots"]:
            entry["content_hash"] = original_hash if entry["version"] == 1 \
                else digest(entry["rows"])
        data["snapshots"][0]["rows"][0]["age"] = 1
        box, _path = self._load_dict(data)
        # v1 摘要校验失败无法进入；v2 因替代目标缺失也被隔离
        self.assertEqual(len(box.snapshots), 0)
        self.assertEqual({row["version"] for row in box.rejected_imports}, {1, 2})
        reasons = {row["version"]: row["reason"] for row in box.rejected_imports}
        self.assertEqual(reasons[1], REJECT_CORRUPT)
        self.assertEqual(reasons[2], REJECT_REPLACES_MISSING)
        # 引用被隔离快照的任务与作业一并隔离，但课程/同意等账目正常恢复
        self.assertEqual(len(box.tasks), 0)
        self.assertEqual(len(box.assignments), 0)
        self.assertIn("CASE-AML", box.cases)
        self.assertIn("CONS-AML-TEACH", box.consents)
        quarantined = {w.get("assignment_id") for w in box.load_warnings
                       if w.get("assignment_id")}
        self.assertEqual(
            quarantined, {"AS-LIN-01", "AS-WANG-01", "AS-GAO-01"})

    def test_duplicate_identical_entry_replays_on_reload(self):
        data = json.loads(Path(SEED).read_text(encoding="utf-8"))
        data["snapshots"].append(copy.deepcopy(data["snapshots"][0]))
        box, _path = self._load_dict(data)
        self.assertEqual(box.rejected_imports, [])
        self.assertEqual(
            [w for w in box.load_warnings if w.get("version") == 1], [])
        self.assertEqual(len(box.snapshots), 2)


if __name__ == "__main__":
    unittest.main()
