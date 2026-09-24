"""医学教学数据沙箱的领域规则。

六类业务记录分账管理，互不混存，彼此只通过不可变版本引用：

- 课程案例 ``cases``：教学目标、病例与课程的多对多关系；
- 临床数据快照 ``snapshots``：病例数据的版本化快照，勘误以“新版本”发布，旧版永不改写；
- 数据使用同意 ``consents``：按病例授权（可限定课程），可撤回，撤回即时生效；
- 脱敏策略 ``policies``：字段变换规则与小样本（k 匿名）阈值；
- 分析环境 ``environments``：工具镜像的版本与摘要；
- 作业产物 ``assignments``：学生结论、导出申请、评分时冻结的环境指纹与复核记录。

限时切片（``sessions``）与披露决策（``exports``）属于运行记录，不并入上述六账。

关键规则：

1. 学生只能在限时沙箱会话中取得与任务教学目标匹配的脱敏数据切片，身份字段永不下发；
2. 导出先过披露检查：会话有效、课程授权有效、同意未撤回、不含身份字段、每组样本数达到 k 阈值；
3. 教师可以按冻结指纹复现实验，但教师身份不具备患者身份读取能力，且只能访问任课课程；
4. 跨校课程到期后，注册关系与沙箱会话自动收回；
5. 病例勘误、工具升级只影响之后创建的新任务；已评分作业保留当时环境指纹，
   另以风险标记标出后续变化；同意撤回则即时阻断相关导出并撤回会话；
6. 任一结论都可溯源到：数据范围、处理步骤、课程授权、教师复核四部分。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

SERVICE_ID = "medical-sandbox"

# 与 fixtures/domain.json 保持一致的状态词汇
STATUS_PENDING = "待授权"
STATUS_SANDBOX = "沙箱运行"
STATUS_REVIEW = "待披露检查"
STATUS_GRADED = "已评分"
STATUS_REVOKED = "已撤权"
EXPORT_APPROVED = "通过"
EXPORT_BLOCKED = "阻断"
ASSIGNMENT_OPEN = "进行中"

# 快照登记结果与被拒导入原因
REGISTER_FIRST = "首次登记"
REJECT_CONTENT = "异内容覆盖"
REJECT_REPLACES_MISSING = "替代目标缺失"
REJECT_REPLACES_ORDER = "替代关系乱序"
REJECT_REPLACES_CYCLE = "替代关系成环"
REJECT_CORRUPT = "快照内容损坏"


class SandboxError(Exception):
    """沙箱规则违例的基类。"""


class NotFound(SandboxError):
    """引用了不存在的记录。"""


class AuthorizationError(SandboxError):
    """身份不具备所需能力，或授权已失效。"""


class SnapshotRegistrationError(SandboxError):
    """快照登记违例：异内容覆盖、重放不一致或替代链非法。

    ``reason`` 取本模块的 ``REJECT_*`` / ``REPLAY_*`` 常量；
    ``diff`` 列出具体不一致的身份字段；冲突导入本身被记入拒绝账，不进入快照账。
    """

    def __init__(self, reason: str, message: str, diff: Optional[list[str]] = None,
                 rejected: Optional[dict] = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.diff = diff or []
        self.rejected = rejected


def parse_time(value: str) -> datetime:
    """解析夹具中的 ISO 时间。"""
    return datetime.fromisoformat(value)


def canon(value: Any) -> str:
    """供摘要计算的规范化 JSON。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any, length: int = 16) -> str:
    return hashlib.sha256(canon(value).encode("utf-8")).hexdigest()[:length]


def _apply_transform(value: Any, rule: str) -> Any:
    if rule == "drop":
        return None
    if rule.startswith("generalize:"):
        width = int(rule.split(":", 1)[1])
        if isinstance(value, (int, float)):
            lower = int(value) // width * width
            return f"{lower}-{lower + width - 1}"
    return value


class Sandbox:
    """保存六类分账记录并执行全部治理规则。"""

    def __init__(self) -> None:
        self.actors: dict[str, dict] = {}
        self.courses: dict[str, dict] = {}
        self.cases: dict[str, dict] = {}
        # 快照按 (病例, 版本) 存放，任何版本一经发布不可改写
        self.snapshots: dict[tuple[str, int], dict] = {}
        self.consents: dict[str, dict] = {}
        self.policies: dict[str, dict] = {}
        # 分析环境按 (镜像, 版本) 存放；任务创建时钉住当时版本，升级不回溯
        self.environment_versions: dict[tuple[str, int], dict] = {}
        self.tasks: dict[str, dict] = {}
        self.assignments: dict[str, dict] = {}
        self.sessions: dict[str, dict] = {}
        self.exports: dict[str, dict] = {}
        self.enrollments: list[dict] = []
        self.teacher_courses: list[dict] = []
        self.audit: list[dict] = []
        # 被拒绝的快照导入（异内容覆盖、重放不一致、非法替代链、并发抢登）；
        # 只追加，不覆盖原快照——审计与追溯视图从这里取“被拒绝的导入”
        self.rejected_imports: list[dict] = []
        # 夹具/持久化装载时被隔离的条目（与 rejected_imports 对应，供重启巡检）
        self.load_warnings: list[dict] = []
        # 同一 (病例, 版本) 的登记必须串行判定：并发导入时只有一个首次提交者
        self._snapshot_locks: dict[tuple[str, int], threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._seq = 0

    # ---- 装载 -----------------------------------------------------------

    @classmethod
    def from_seed(cls, path: str | Path) -> "Sandbox":
        """从种子夹具装载一个可联调的沙箱。"""
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        box = cls()
        for actor in data.get("actors", []):
            box.actors[actor["id"]] = dict(actor)
        for course in data.get("courses", []):
            box.add_course(
                course["id"], course["name"],
                starts_at=parse_time(course["starts_at"]),
                ends_at=parse_time(course["ends_at"]),
                institutions=course.get("institutions"),
                cross_institutional=course.get("cross_institutional", False),
            )
        for case in data.get("cases", []):
            box.add_case(case["id"], case["title"],
                         description=case.get("description", ""),
                         courses=case.get("courses"))
        for policy in data.get("policies", []):
            box.policies[policy["id"]] = dict(policy)
        for env in data.get("environments", []):
            box.add_environment(
                env["id"], env["version"], env["tools"],
                released_at=parse_time(env["released_at"]), note=env.get("note", ""),
            )
        for snapshot in data.get("snapshots", []):
            # 装载路径与在线登记走同一套身份与替代链校验：坏快照被隔离进
            # rejected_imports，其余账目照常恢复（重启后不被坏数据污染）
            try:
                box.add_snapshot(
                    snapshot["case_id"], snapshot["version"], snapshot["rows"],
                    identity_fields=snapshot.get("identity_fields", []),
                    released_at=parse_time(snapshot["released_at"]),
                    replaces=snapshot.get("replaces"),
                    erratum_note=snapshot.get("erratum_note", ""),
                    submitted_by=snapshot.get("submitted_by", "种子装载"),
                    expected_content_hash=snapshot.get("content_hash"),
                )
            except SnapshotRegistrationError as exc:
                box.load_warnings.append({
                    "case_id": snapshot.get("case_id"),
                    "version": snapshot.get("version"),
                    "reason": exc.reason,
                    "diff": exc.diff,
                    "message": str(exc),
                })
        for grant in data.get("consents", []):
            box.grant_consent(
                grant["id"], grant["case_id"],
                course_id=grant.get("course_id"),
                granted_at=parse_time(grant["granted_at"]),
                scope_note=grant.get("scope_note", ""),
            )
        for enrollment in data.get("enrollments", []):
            box.enroll(enrollment["student_id"], enrollment["course_id"],
                       status=enrollment.get("status"))
        for link in data.get("teachers", []):
            box.assign_teacher(link["teacher_id"], link["course_id"])
        for task in data.get("tasks", []):
            # 引用了被隔离快照/环境的任务一并隔离，不能让一条坏数据阻断整库恢复
            try:
                box.create_task(
                    task["id"], task["course_id"], task["case_id"],
                    task["objective_fields"], task["policy_id"], task["environment_id"],
                    now=parse_time(task["created_at"]),
                    snapshot_version=task.get("snapshot_version"),
                )
            except SandboxError as exc:
                box.load_warnings.append({
                    "task_id": task.get("id"), "reason": type(exc).__name__,
                    "message": str(exc),
                })
        for assignment in data.get("assignments", []):
            if assignment["task_id"] not in box.tasks:
                box.load_warnings.append({
                    "assignment_id": assignment.get("id"),
                    "reason": "任务缺失",
                    "message": f"作业 {assignment.get('id')} 引用的任务未通过装载校验，"
                               f"一并隔离",
                })
                continue
            box._load_assignment(assignment)
        box._reconcile_flags()
        return box

    def _load_assignment(self, data: dict) -> None:
        task = self.tasks[data["task_id"]]
        recipe = data.get("recipe", [])
        assignment = {
            "id": data["id"],
            "task_id": data["task_id"],
            "student_id": data["student_id"],
            "conclusion": data["conclusion"],
            "recipe": recipe,
            "result_hash": data.get("result_hash"),
            "status": data["status"],
            "submitted_at": parse_time(data["submitted_at"]),
            "graded_at": parse_time(data["graded_at"]) if data.get("graded_at") else None,
            "grade": data.get("grade"),
            "fingerprint": data.get("fingerprint"),
            "reviews": data.get("reviews", []),
            "risk_flags": data.get("risk_flags", []),
            "slice_ids": data.get("slice_ids", []),
            "export_ids": data.get("export_ids", []),
        }
        # 已评分作业：指纹与结果摘要从各版本账重算，保证“当时环境”可独立验证
        if assignment["status"] == STATUS_GRADED:
            if assignment["fingerprint"] is None:
                assignment["fingerprint"] = self.environment_fingerprint(task, assignment)
            if not assignment["result_hash"]:
                assignment["result_hash"] = digest(
                    {"rows": self._project_rows(task), "recipe": recipe})
        self.assignments[assignment["id"]] = assignment

    def _reconcile_flags(self) -> None:
        """装载后对账：给已评分作业补上评分之后发生的勘误、工具升级、同意撤回标记。"""
        for assignment in self.assignments.values():
            if assignment["status"] != STATUS_GRADED:
                continue
            task = self.tasks[assignment["task_id"]]
            graded_at = assignment["graded_at"]
            fp = assignment["fingerprint"]
            newer_snapshots = sorted(
                (snap for (cid, ver), snap in self.snapshots.items()
                 if cid == task["case_id"] and ver > fp["snapshot"]["version"]
                 and snap["released_at"] > graded_at),
                key=lambda snap: snap["version"])
            for snap in newer_snapshots:
                self._flag(assignment, {
                    "type": "病例勘误",
                    "at": snap["released_at"].isoformat(),
                    "detail": snap["erratum_note"]
                    or f"病例已发布 v{snap['version']}，结论基于 v{fp['snapshot']['version']}",
                    "current_version": snap["version"],
                })
            newer_envs = sorted(
                (rec for (eid, ver), rec in self.environment_versions.items()
                 if eid == task["environment_id"] and ver > fp["environment"]["version"]
                 and rec["released_at"] > graded_at),
                key=lambda rec: rec["version"])
            for rec in newer_envs:
                self._flag(assignment, {
                    "type": "工具升级",
                    "at": rec["released_at"].isoformat(),
                    "detail": rec["note"] or f"分析环境已升级到 v{rec['version']}",
                    "current": f"{rec['id']}:v{rec['version']}",
                })
            for grant in self.consents.values():
                if grant["case_id"] != task["case_id"]:
                    continue
                if grant["course_id"] is not None and grant["course_id"] != task["course_id"]:
                    continue
                if grant["withdrawn_at"]:
                    self._flag(assignment, {
                        "type": "同意撤回",
                        "at": grant["withdrawn_at"].isoformat(),
                        "detail": f"授权 {grant['id']} 已撤回，结论所依据的授权不再有效",
                        "consent_id": grant["id"],
                    })

    # ---- 基础登记 -------------------------------------------------------

    def _new_id(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}-{self._seq:04d}"

    def add_course(self, course_id: str, name: str, starts_at: datetime,
                   ends_at: datetime, institutions: Optional[list[str]] = None,
                   cross_institutional: bool = False) -> None:
        self.courses[course_id] = {
            "id": course_id,
            "name": name,
            "starts_at": starts_at,
            "ends_at": ends_at,
            "institutions": institutions or [],
            "cross_institutional": cross_institutional or len(institutions or []) > 1,
        }

    def add_case(self, case_id: str, title: str, description: str = "",
                 courses: Optional[list[str]] = None) -> None:
        self.cases[case_id] = {
            "id": case_id,
            "title": title,
            "description": description,
            "courses": list(courses or []),
        }

    def add_snapshot(self, case_id: str, version: int, rows: list[dict],
                     identity_fields: list[str], released_at: datetime,
                     replaces: Optional[int] = None,
                     erratum_note: str = "",
                     submitted_by: Optional[str] = None,
                     submitted_at: Optional[datetime] = None,
                     expected_content_hash: Optional[str] = None) -> dict:
        """登记一个病例快照版本。

        ``(病例编号, 版本号)`` 是不可变身份。身份由五部分共同确定：
        规范化内容摘要、身份字段集合、发布时间、替代关系、勘误说明。

        - 身份完全一致的重复提交：幂等返回原记录（精确重放），任何账目不改写；
        - 任一部分不一致：拒绝登记，原样保留既有快照、任务、切片、导出与已评分
          作业，冲突导入进入 ``rejected_imports`` 并抛 :class:`SnapshotRegistrationError`；
        - ``replaces`` 指向的版本必须存在、版本号必须更低且沿替代链不成环，
          否则拒绝登记；
        - 同一 ``(病例, 版本)`` 的并发登记串行判定，只有一个首次提交者；
        - 合法新版本沿完整替代链给引用链上任一旧版本的已评分作业追加风险标记。
        """
        if case_id not in self.cases:
            raise NotFound(f"未知病例：{case_id}")
        submitted_by = submitted_by or "数据管理员"
        submitted_at = submitted_at or released_at
        candidate = self._snapshot_identity(
            case_id, version, rows, identity_fields, released_at,
            replaces, erratum_note)

        # 同一身份键的判定必须串行：并发导入同一版本时只有一个首次提交者
        with self._snapshot_lock((case_id, version)):
            # 装载恢复时，行数据必须与登记时固化的规范化摘要一致；
            # 存储被篡改（同版本异内容）在这里即被隔离，不进入快照账
            if (expected_content_hash is not None
                    and expected_content_hash != candidate["content_hash"]):
                self._reject_import(
                    REJECT_CORRUPT, submitted_by, submitted_at,
                    {**candidate, "expected_content_hash": expected_content_hash},
                    None, ["content_hash"],
                    f"病例 {case_id} v{version} 的内容摘要与登记摘要不符，"
                    f"存储可能被篡改或损坏")
            existing = self.snapshots.get((case_id, version))
            if existing is not None:
                existing_identity = self._stored_identity(existing)
                if canon(existing_identity) == canon(candidate):
                    # 精确重放：幂等返回，首次提交者与既有账目不被改写
                    self.audit.append({
                        "at": submitted_at.isoformat(), "action": "快照精确重放",
                        "case_id": case_id, "version": version,
                        "submitted_by": submitted_by,
                        "first_submitted_by": existing["registered_by"],
                    })
                    return existing
                diff = [name for name in candidate
                        if canon(existing_identity.get(name)) != canon(candidate[name])]
                self._reject_import(
                    REJECT_CONTENT, submitted_by, submitted_at, candidate,
                    existing_identity, diff,
                    f"病例 {case_id} 版本 v{version} 已由 "
                    f"{existing['registered_by']} 提交且内容不可变；"
                    f"冲突字段：{','.join(diff)}")
                # _reject_import 抛出异常，以下存储逻辑不会执行

            # 新身份：先校验替代关系，乱序/成环/目标缺失一律不得进入存储
            self._validate_replaces(case_id, version, replaces,
                                    submitted_by, submitted_at, candidate)

            record = {
                "case_id": case_id,
                "version": version,
                "rows": rows,
                "identity_fields": list(identity_fields),
                "released_at": released_at,
                "replaces": replaces,
                "erratum_note": erratum_note,
                "content_hash": digest(rows),
                "registered_by": submitted_by,
                "register_status": REGISTER_FIRST,
            }
            self.snapshots[(case_id, version)] = record
            self.audit.append({
                "at": submitted_at.isoformat(), "action": "快照首次登记",
                "case_id": case_id, "version": version,
                "submitted_by": submitted_by, "replaces": replaces,
                "content_hash": record["content_hash"],
            })
            # 影响面标记与登记在同一临界区：沿完整替代链找引用旧版本的作业，
            # v3 替代 v2、v2 替代 v1 时，基于 v1 的作业同样面对 v3 的新数据。
            # _flag 按作业幂等，精确重放不会重复追加。
            if replaces is not None:
                chain = self._replaces_chain(case_id, replaces)
                for assignment in self.assignments.values():
                    fp = assignment.get("fingerprint")
                    if (fp and fp["snapshot"]["case_id"] == case_id
                            and fp["snapshot"]["version"] in chain):
                        self._flag(assignment, {
                            "type": "病例勘误",
                            "at": released_at.isoformat(),
                            "detail": erratum_note
                            or f"病例已发布 v{version}，结论基于 v{fp['snapshot']['version']}",
                            "current_version": version,
                        })
        return record

    def _snapshot_lock(self, key: tuple[str, int]) -> threading.Lock:
        with self._locks_guard:
            lock = self._snapshot_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._snapshot_locks[key] = lock
            return lock

    @staticmethod
    def _snapshot_identity(case_id: str, version: int, rows: list[dict],
                           identity_fields: list[str], released_at: datetime,
                           replaces: Optional[int],
                           erratum_note: str) -> dict:
        """构成快照不可变身份的五个部分（病例与版本为身份键）。"""
        return {
            "case_id": case_id,
            "version": version,
            "content_hash": digest(rows),              # 规范化内容
            "identity_fields": sorted(identity_fields),
            "released_at": released_at.isoformat(),    # 发布时间
            "replaces": replaces,                      # 替代关系
            "erratum_note": erratum_note,              # 勘误说明
        }

    def _stored_identity(self, record: dict) -> dict:
        return self._snapshot_identity(
            record["case_id"], record["version"], record["rows"],
            record["identity_fields"], record["released_at"],
            record["replaces"], record["erratum_note"])

    def _validate_replaces(self, case_id: str, version: int,
                           replaces: Optional[int], submitted_by: str,
                           submitted_at: Optional[datetime],
                           candidate: dict) -> None:
        if replaces is None:
            return
        if replaces == version:
            self._reject_import(
                REJECT_REPLACES_CYCLE, submitted_by, submitted_at, candidate,
                None, ["replaces"],
                f"病例 {case_id} v{version} 不能替代自身")
        if replaces > version:
            self._reject_import(
                REJECT_REPLACES_ORDER, submitted_by, submitted_at, candidate,
                None, ["replaces"],
                f"病例 {case_id} v{version} 不能替代更高版本 v{replaces}")
        if (case_id, replaces) not in self.snapshots:
            self._reject_import(
                REJECT_REPLACES_MISSING, submitted_by, submitted_at, candidate,
                None, ["replaces"],
                f"病例 {case_id} v{version} 的替代目标 v{replaces} 不存在")
        # 沿替代链前行，若回到当前版本则成环（含经由历史节点的闭环）
        seen: set[int] = set()
        cursor = replaces
        while cursor is not None:
            if cursor == version:
                self._reject_import(
                    REJECT_REPLACES_CYCLE, submitted_by, submitted_at, candidate,
                    None, ["replaces"],
                    f"病例 {case_id} 的替代关系在 v{version} 处成环")
            if cursor in seen:  # 防御：历史链本身异常
                self._reject_import(
                    REJECT_REPLACES_CYCLE, submitted_by, submitted_at, candidate,
                    None, ["replaces"],
                    f"病例 {case_id} 的替代关系经过 v{cursor} 时成环")
            seen.add(cursor)
            cursor = self.snapshots[(case_id, cursor)]["replaces"]

    def _replaces_chain(self, case_id: str, version: int) -> set[int]:
        """返回 v``version`` 及其沿替代关系追溯到的全部旧版本号。"""
        chain: set[int] = set()
        cursor: Optional[int] = version
        while cursor is not None and cursor not in chain:
            chain.add(cursor)
            record = self.snapshots.get((case_id, cursor))
            cursor = record["replaces"] if record else None
        return chain

    def _reject_import(self, reason: str, submitted_by: str,
                       submitted_at: Optional[datetime], candidate: dict,
                       existing: Optional[dict], diff: list[str],
                       message: str) -> None:
        """把被拒绝的导入只追加进拒绝账与审计账，然后抛出登记违例。"""
        entry = {
            "reason": reason,
            "at": (submitted_at.isoformat() if submitted_at else None),
            "submitted_by": submitted_by,
            "case_id": candidate.get("case_id"),
            "version": candidate.get("version"),
            "candidate": candidate,
            "existing": existing,
            "diff": diff,
            "message": message,
        }
        self.rejected_imports.append(entry)
        self.audit.append({
            "at": entry["at"], "action": "快照导入被拒",
            "case_id": entry["case_id"], "version": entry["version"],
            "reason": reason, "diff": diff, "submitted_by": submitted_by,
        })
        raise SnapshotRegistrationError(reason, message, diff=diff, rejected=entry)

    def add_environment(self, env_id: str, version: int, tools: dict[str, str],
                        released_at: datetime, note: str = "") -> dict:
        record = {
            "id": env_id,
            "version": version,
            "tools": dict(tools),
            "released_at": released_at,
            "digest": digest(tools),
            "note": note,
        }
        self.environment_versions[(env_id, version)] = record
        # 升级：给引用旧版本的已评分作业追加风险标记（前向影响、不冻结改写）
        prior = [v for (eid, v), rec in self.environment_versions.items()
                 if eid == env_id and v < version]
        if prior:
            old_version = max(prior)
            for assignment in self.assignments.values():
                fp = assignment.get("fingerprint")
                if (fp and fp["environment"]["id"] == env_id
                        and fp["environment"]["version"] == old_version):
                    self._flag(assignment, {
                        "type": "工具升级",
                        "at": released_at.isoformat(),
                        "detail": note or f"分析环境已升级到 v{version}",
                        "current": f"{env_id}:v{version}",
                    })
        return record

    def _environment(self, env_id: str, version: int) -> dict:
        record = self.environment_versions.get((env_id, version))
        if record is None:
            raise NotFound(f"分析环境 {env_id}:v{version} 不存在")
        return record

    def grant_consent(self, consent_id: str, case_id: str,
                      course_id: Optional[str], granted_at: datetime,
                      scope_note: str = "") -> dict:
        grant = {
            "id": consent_id,
            "case_id": case_id,
            "course_id": course_id,  # None 表示覆盖所有教学用途
            "granted_at": granted_at,
            "withdrawn_at": None,
            "scope_note": scope_note,
        }
        self.consents[consent_id] = grant
        return grant

    def enroll(self, student_id: str, course_id: str,
               status: str = STATUS_PENDING) -> dict:
        record = {"student_id": student_id, "course_id": course_id, "status": status}
        self.enrollments.append(record)
        return record

    def assign_teacher(self, teacher_id: str, course_id: str) -> None:
        self.teacher_courses.append(
            {"teacher_id": teacher_id, "course_id": course_id})

    def create_task(self, task_id: str, course_id: str, case_id: str,
                    objective_fields: list[str], policy_id: str,
                    environment_id: str, now: datetime,
                    snapshot_version: Optional[int] = None) -> dict:
        if course_id not in self.courses:
            raise NotFound(f"未知课程：{course_id}")
        if case_id not in self.cases:
            raise NotFound(f"未知病例：{case_id}")
        if snapshot_version is None:
            # 任务创建时钉住“当时最新”的快照版本；之后勘误不改变本任务
            available = [v for (cid, v), snap in self.snapshots.items()
                         if cid == case_id and snap["released_at"] <= now]
            if not available:
                raise SandboxError(f"病例 {case_id} 在 {now} 尚无已发布快照")
            snapshot_version = max(available)
        if (case_id, snapshot_version) not in self.snapshots:
            raise NotFound(f"快照 {case_id}:v{snapshot_version} 不存在")
        # 任务创建时钉住“当时最新”的分析环境版本；之后工具升级不回溯本任务
        env_versions = [v for (eid, v), rec in self.environment_versions.items()
                        if eid == environment_id and rec["released_at"] <= now]
        if not env_versions:
            raise SandboxError(f"分析环境 {environment_id} 在 {now} 尚无已发布版本")
        environment_version = max(env_versions)
        if course_id not in self.cases[case_id]["courses"]:
            self.cases[case_id]["courses"].append(course_id)
        task = {
            "id": task_id,
            "course_id": course_id,
            "case_id": case_id,
            "objective_fields": list(objective_fields),
            "snapshot_version": snapshot_version,
            "policy_id": policy_id,
            "environment_id": environment_id,
            "environment_version": environment_version,
            "created_at": now,
        }
        self.tasks[task_id] = task
        return task

    # ---- 授权判定 -------------------------------------------------------

    def _enrollment(self, student_id: str, course_id: str) -> Optional[dict]:
        for record in self.enrollments:
            if record["student_id"] == student_id and record["course_id"] == course_id:
                return record
        return None

    def _is_teacher(self, teacher_id: str, course_id: str) -> bool:
        return any(link["teacher_id"] == teacher_id and link["course_id"] == course_id
                   for link in self.teacher_courses)

    def _active_consents(self, case_id: str, course_id: str,
                         now: datetime) -> list[dict]:
        """返回当下覆盖该病例+课程且未撤回的同意。"""
        active = []
        for grant in self.consents.values():
            if grant["case_id"] != case_id:
                continue
            if grant["course_id"] is not None and grant["course_id"] != course_id:
                continue
            if grant["granted_at"] > now:
                continue
            if grant["withdrawn_at"] is not None and grant["withdrawn_at"] <= now:
                continue
            active.append(grant)
        return active

    def _course_open(self, course_id: str, now: datetime) -> bool:
        course = self.courses[course_id]
        return course["starts_at"] <= now < course["ends_at"]

    def sweep_expired(self, now: datetime) -> list[str]:
        """到期收权：课程窗口结束后，注册关系与进行中的沙箱会话自动收回。

        幂等；返回本次新收回的注册记录标识。跨校课程同样适用，且到期后无宽限。
        """
        revoked = []
        for enrollment in self.enrollments:
            if enrollment["status"] == STATUS_REVOKED:
                continue
            if not self._course_open(enrollment["course_id"], now):
                enrollment["status"] = STATUS_REVOKED
                revoked.append(f"{enrollment['student_id']}@{enrollment['course_id']}")
                for session in self.sessions.values():
                    task = self.tasks[session["task_id"]]
                    if (session["student_id"] == enrollment["student_id"]
                            and task["course_id"] == enrollment["course_id"]
                            and session["status"] == STATUS_SANDBOX):
                        self._revoke_session(session, "课程到期", now)
        return revoked

    # ---- 限时数据切片 ---------------------------------------------------

    def _project_rows(self, task: dict) -> list[dict]:
        """按教学目标字段与脱敏策略投影数据；身份字段一律剔除。"""
        snapshot = self.snapshots[(task["case_id"], task["snapshot_version"])]
        policy = self.policies[task["policy_id"]]
        identity = set(snapshot["identity_fields"])
        allowed = [f for f in task["objective_fields"] if f not in identity]
        projected = []
        for row in snapshot["rows"]:
            item = {}
            for field in allowed:
                rule = policy.get("transforms", {}).get(field)
                value = row.get(field)
                if rule is not None:
                    value = _apply_transform(value, rule)
                if value is not None:
                    item[field] = value
            projected.append(item)
        return projected

    def issue_slice(self, student_id: str, task_id: str, now: datetime,
                    ttl_minutes: int = 240) -> dict:
        """在限时沙箱中签发与任务教学目标匹配的数据切片。"""
        task = self.tasks.get(task_id)
        if task is None:
            raise NotFound(f"未知任务：{task_id}")
        course_id = task["course_id"]
        enrollment = self._enrollment(student_id, course_id)
        if enrollment is None:
            raise AuthorizationError(f"学生 {student_id} 未注册课程 {course_id}")
        self.sweep_expired(now)
        if enrollment["status"] == STATUS_REVOKED or not self._course_open(course_id, now):
            raise AuthorizationError(f"课程 {course_id} 授权已到期收回")
        if not self._active_consents(task["case_id"], course_id, now):
            raise AuthorizationError(f"病例 {task['case_id']} 缺少有效数据使用同意")

        slice_id = self._new_id("SLICE")
        session = {
            "id": slice_id,
            "task_id": task_id,
            "student_id": student_id,
            "rows": self._project_rows(task),
            "issued_at": now,
            "expires_at": now + timedelta(minutes=ttl_minutes),
            "status": STATUS_SANDBOX,
            "revoke_reason": None,
        }
        self.sessions[slice_id] = session
        return session

    def _revoke_session(self, session: dict, reason: str, now: datetime) -> None:
        session["status"] = STATUS_REVOKED
        session["revoke_reason"] = reason
        self.audit.append({"at": now.isoformat(), "action": "会话撤回",
                           "slice_id": session["id"], "reason": reason})

    def _session_live(self, session: dict, now: datetime) -> Optional[str]:
        if session["status"] == STATUS_REVOKED:
            return session["revoke_reason"] or "会话已撤回"
        if now >= session["expires_at"]:
            return "切片过期"
        return None

    # ---- 作业与环境指纹 -------------------------------------------------

    def submit_assignment(self, assignment_id: str, student_id: str, task_id: str,
                          conclusion: str, recipe: list[dict], now: datetime,
                          slice_ids: Optional[list[str]] = None) -> dict:
        task = self.tasks.get(task_id)
        if task is None:
            raise NotFound(f"未知任务：{task_id}")
        rows = []
        for slice_id in slice_ids or []:
            session = self.sessions.get(slice_id)
            if session is None:
                raise NotFound(f"未知切片：{slice_id}")
            if session["student_id"] != student_id or session["task_id"] != task_id:
                raise AuthorizationError("切片不属于当前学生或任务")
            if self._session_live(session, now):
                raise AuthorizationError("引用的沙箱切片已失效，不能用于提交")
            rows = session["rows"]
        result_hash = digest({"rows": rows, "recipe": recipe})
        assignment = {
            "id": assignment_id,
            "task_id": task_id,
            "student_id": student_id,
            "conclusion": conclusion,
            "recipe": recipe,
            "result_hash": result_hash,
            "status": ASSIGNMENT_OPEN,
            "submitted_at": now,
            "graded_at": None,
            "grade": None,
            "fingerprint": None,
            "reviews": [],
            "risk_flags": [],
            "slice_ids": list(slice_ids or []),
            "export_ids": [],
        }
        self.assignments[assignment_id] = assignment
        return assignment

    def environment_fingerprint(self, task: dict, assignment: dict) -> dict:
        """评分时冻结的环境指纹：数据版本、脱敏策略、工具镜像、处理步骤四要素齐备。"""
        snapshot = self.snapshots[(task["case_id"], task["snapshot_version"])]
        policy = self.policies[task["policy_id"]]
        env = self._environment(task["environment_id"], task["environment_version"])
        return {
            "snapshot": {
                "case_id": task["case_id"],
                "version": snapshot["version"],
                "content_hash": snapshot["content_hash"],
            },
            "policy": {"id": policy["id"], "version": policy["version"],
                       "hash": digest({k: v for k, v in policy.items() if k != "id"})},
            "environment": {"id": env["id"], "version": env["version"],
                            "digest": env["digest"]},
            "recipe": assignment["recipe"],
            "recipe_hash": digest(assignment["recipe"]),
        }

    def grade_assignment(self, teacher_id: str, assignment_id: str, grade: str,
                         review_note: str, now: datetime) -> dict:
        """教师评分：冻结当时环境指纹并留下复核记录；此后作业内容不可改写。"""
        assignment = self.assignments.get(assignment_id)
        if assignment is None:
            raise NotFound(f"未知作业：{assignment_id}")
        task = self.tasks[assignment["task_id"]]
        if not self._is_teacher(teacher_id, task["course_id"]):
            raise AuthorizationError("只有任课教师可以评分")
        if assignment["status"] == STATUS_GRADED:
            raise SandboxError("作业已评分，评分记录只能追加不能覆盖")
        assignment["status"] = STATUS_GRADED
        assignment["grade"] = grade
        assignment["graded_at"] = now
        assignment["fingerprint"] = self.environment_fingerprint(task, assignment)
        assignment["reviews"].append({
            "teacher_id": teacher_id, "at": now.isoformat(),
            "grade": grade, "note": review_note, "stage": "评分",
        })
        # 评分时刻之后才出现的风险由勘误/升级/撤回事件追加
        return assignment

    @staticmethod
    def _flag(assignment: dict, flag: dict) -> None:
        if any(existing.get("type") == flag["type"]
               and existing.get("current_version") == flag.get("current_version")
               and existing.get("at") == flag.get("at")
               for existing in assignment["risk_flags"]):
            return
        assignment["risk_flags"].append(flag)

    # ---- 导出与披露检查 -------------------------------------------------

    def request_export(self, export_id: str, student_id: str, slice_id: str,
                       groups: list[dict], columns: list[str], now: datetime,
                       assignment_id: Optional[str] = None) -> dict:
        """提交导出申请并立即执行披露检查；小样本等情形返回“阻断”而非放行。"""
        session = self.sessions.get(slice_id)
        if session is None:
            raise NotFound(f"未知切片：{slice_id}")
        if session["student_id"] != student_id:
            raise AuthorizationError("切片不属于该学生")
        task = self.tasks[session["task_id"]]
        policy = self.policies[task["policy_id"]]
        record = {
            "id": export_id,
            "slice_id": slice_id,
            "assignment_id": assignment_id,
            "student_id": student_id,
            "groups": groups,
            "columns": columns,
            "requested_at": now,
            "decision": STATUS_REVIEW,
            "reasons": [],
            "decided_at": None,
        }
        self.exports[export_id] = record
        self._disclose(record, now)
        if assignment_id and assignment_id in self.assignments:
            self.assignments[assignment_id]["export_ids"].append(export_id)
        return record

    def _disclose(self, record: dict, now: datetime) -> None:
        session = self.sessions[record["slice_id"]]
        task = self.tasks[session["task_id"]]
        reasons: list[str] = []

        dead = self._session_live(session, now)
        if dead:
            reasons.append(dead)
        enrollment = self._enrollment(record["student_id"], task["course_id"])
        if enrollment is None or enrollment["status"] == STATUS_REVOKED \
                or not self._course_open(task["course_id"], now):
            reasons.append("课程授权失效")
        if not self._active_consents(task["case_id"], task["course_id"], now):
            reasons.append("同意撤回")

        snapshot = self.snapshots[(task["case_id"], task["snapshot_version"])]
        forbidden = set(snapshot["identity_fields"])
        leaked = sorted(forbidden.intersection(record["columns"]))
        if leaked:
            reasons.append(f"含身份字段：{','.join(leaked)}")

        threshold = self.policies[task["policy_id"]]["k_threshold"]
        for group in record["groups"]:
            if group["count"] < threshold:
                reasons.append(
                    f"小样本组 {group['key']} 仅 {group['count']} 例（k≥{threshold}）")

        record["reasons"] = reasons
        record["decision"] = EXPORT_BLOCKED if reasons else EXPORT_APPROVED
        record["decided_at"] = now
        self.audit.append({
            "at": now.isoformat(), "action": "披露检查",
            "export_id": record["id"], "decision": record["decision"],
            "reasons": reasons,
        })

    # ---- 同意撤回：即时效力 ---------------------------------------------

    def withdraw_consent(self, consent_id: str, now: datetime) -> dict:
        """撤回同意：进行中的相关会话立即撤回，待披露导出阻断，受影响作业列清。

        已评分作业不被删除或改写，保留环境指纹并追加“同意撤回”风险标记。
        """
        grant = self.consents.get(consent_id)
        if grant is None:
            raise NotFound(f"未知同意记录：{consent_id}")
        if grant["withdrawn_at"] is not None:
            raise SandboxError("同意已撤回，不能重复撤回")
        grant["withdrawn_at"] = now

        affected: list[dict] = []
        for task in self.tasks.values():
            if task["case_id"] != grant["case_id"]:
                continue
            if grant["course_id"] is not None and task["course_id"] != grant["course_id"]:
                continue
            # 撤回该病例相关的进行中会话
            for session in self.sessions.values():
                if session["task_id"] == task["id"] and session["status"] == STATUS_SANDBOX:
                    self._revoke_session(session, "同意撤回", now)
            # 仍在披露队列中的导出立即阻断
            for export in self.exports.values():
                sess = self.sessions[export["slice_id"]]
                if sess["task_id"] == task["id"] and export["decision"] != EXPORT_BLOCKED:
                    export["decision"] = EXPORT_BLOCKED
                    export["reasons"] = list(dict.fromkeys(
                        export["reasons"] + ["同意撤回"]))
                    export["decided_at"] = now
            for assignment in self.assignments.values():
                if assignment["task_id"] != task["id"]:
                    continue
                impact = ("已评分：保留冻结指纹并标记风险"
                          if assignment["status"] == STATUS_GRADED
                          else "未评分：会话撤回且导出阻断")
                if assignment["status"] == STATUS_GRADED:
                    self._flag(assignment, {
                        "type": "同意撤回",
                        "at": now.isoformat(),
                        "detail": f"授权 {consent_id} 已撤回，结论所依据的授权不再有效",
                        "consent_id": consent_id,
                    })
                affected.append({
                    "assignment_id": assignment["id"],
                    "course_id": task["course_id"],
                    "student_id": assignment["student_id"],
                    "status": assignment["status"],
                    "impact": impact,
                })
        report = {
            "consent_id": consent_id,
            "case_id": grant["case_id"],
            "withdrawn_at": now.isoformat(),
            "assignments": affected,
        }
        self.audit.append({"at": now.isoformat(), "action": "同意撤回", **report})
        return report

    # ---- 教师复现与身份隔离 ---------------------------------------------

    def read_patient_identity(self, actor_id: str, case_id: str) -> None:
        """尝试读取患者身份字段。

        学生、教师角色没有 ``patient_identity:read`` 能力；即便某身份持有该
        临床侧能力，教学沙箱也不会在任何教学会话中下发身份字段。两道闸缺一不可。
        """
        actor = self.actors.get(actor_id, {"roles": []})
        if "patient_identity:read" not in actor.get("roles", []):
            raise AuthorizationError(
                f"身份 {actor_id} 无权读取病例 {case_id} 的患者身份字段")
        raise AuthorizationError(
            f"教学沙箱不向任何会话下发病例 {case_id} 的患者身份字段")

    def reproduce_report(self, teacher_id: str, assignment_id: str,
                         now: datetime) -> dict:
        """教师按评分时冻结的指纹复现实验：先校验任课身份，再在冻结环境中重放。

        教师可以复现实验，但全程只有脱敏视图；非任课课程的作业不可访问。
        """
        assignment = self.assignments.get(assignment_id)
        if assignment is None:
            raise NotFound(f"未知作业：{assignment_id}")
        if assignment["status"] != STATUS_GRADED:
            raise SandboxError("只能复现已评分作业，以保证指纹已冻结")
        task = self.tasks[assignment["task_id"]]
        if not self._is_teacher(teacher_id, task["course_id"]):
            raise AuthorizationError("非任课教师不能复现该课程作业")

        snapshot = self.snapshots[(task["case_id"], task["snapshot_version"])]
        env = self._environment(task["environment_id"], task["environment_version"])
        # 教师在能力层面即被拒绝读取患者身份（独立于课程授权的一道闸）
        actor = self.actors.get(teacher_id, {"roles": []})
        identity_denied = "patient_identity:read" not in actor.get("roles", [])

        # 在冻结环境中重放：每个步骤所用工具必须存在于当时镜像
        missing = [step["tool"] for step in assignment["recipe"]
                   if step["tool"] not in env["tools"]]
        rerun_rows = self._project_rows(task)
        rerun_hash = digest({"rows": rerun_rows, "recipe": assignment["recipe"]})
        match = not missing and rerun_hash == assignment["result_hash"]

        current_fp = self.environment_fingerprint(task, assignment)
        return {
            "assignment_id": assignment_id,
            "teacher_id": teacher_id,
            "status": "复现一致" if match else "复现不一致",
            "frozen_fingerprint": assignment["fingerprint"],
            "fingerprint_intact": current_fp == assignment["fingerprint"],
            "rerun": {
                "missing_tools": missing,
                "result_hash": rerun_hash,
                "matches_graded": match,
            },
            "identity_access": "拒绝" if identity_denied else "异常放行",
            "identity_fields": snapshot["identity_fields"],
            "risk_flags": assignment["risk_flags"],
        }

    # ---- 结论溯源 -------------------------------------------------------

    def trace(self, assignment_id: str) -> dict:
        """把任一分析结论追到数据范围、处理步骤、课程授权、教师复核四部分。"""
        assignment = self.assignments.get(assignment_id)
        if assignment is None:
            raise NotFound(f"未知作业：{assignment_id}")
        task = self.tasks[assignment["task_id"]]
        course = self.courses[task["course_id"]]
        snapshot = self.snapshots[(task["case_id"], task["snapshot_version"])]
        policy = self.policies[task["policy_id"]]
        env = self._environment(task["environment_id"], task["environment_version"])
        enrollment = self._enrollment(assignment["student_id"], task["course_id"])

        consent_view = []
        for grant in self.consents.values():
            if grant["case_id"] != task["case_id"]:
                continue
            if grant["course_id"] is not None and grant["course_id"] != task["course_id"]:
                continue
            consent_view.append({
                "consent_id": grant["id"],
                "granted_at": grant["granted_at"].isoformat(),
                "withdrawn_at": grant["withdrawn_at"].isoformat()
                if grant["withdrawn_at"] else None,
                "scope": "课程专用" if grant["course_id"] else "教学通用",
            })
        return {
            "assignment_id": assignment_id,
            "conclusion": assignment["conclusion"],
            "数据范围": {
                "case_id": task["case_id"],
                "snapshot_version": snapshot["version"],
                "content_hash": snapshot["content_hash"],
                "objective_fields": task["objective_fields"],
                "slice_ids": assignment["slice_ids"],
            },
            "处理步骤": {
                "policy": {"id": policy["id"], "version": policy["version"]},
                "environment": {"id": env["id"], "version": env["version"],
                                "digest": env["digest"]},
                "recipe": assignment["recipe"],
                "recipe_hash": digest(assignment["recipe"]),
            },
            "课程授权": {
                "course_id": task["course_id"],
                "course_name": course["name"],
                "cross_institutional": course["cross_institutional"],
                "window": [course["starts_at"].isoformat(),
                           course["ends_at"].isoformat()],
                "student_id": assignment["student_id"],
                "enrollment_status": enrollment["status"] if enrollment else None,
                "consents": consent_view,
            },
            "教师复核": list(assignment["reviews"]),
            "风险标记": list(assignment["risk_flags"]),
            "fingerprint": assignment["fingerprint"],
            "快照谱系": self._lineage_payload(
                task["case_id"],
                frozen_version=(assignment["fingerprint"] or {}).get(
                    "snapshot", {}).get("version", task["snapshot_version"])),
        }

    def case_lineage(self, case_id: str) -> dict:
        """病例维度的版本谱系：版本链、当前有效版本、被拒绝的导入。

        供教师在不打开某份作业时解释“旧作业冻结的是哪版数据、现在有效是哪版、
        哪些重传曾被系统拒绝”。
        """
        if case_id not in self.cases:
            raise NotFound(f"未知病例：{case_id}")
        return self._lineage_payload(case_id)

    def _lineage_payload(self, case_id: str,
                         frozen_version: Optional[int] = None) -> dict:
        versions = sorted(
            (snap for (cid, _ver), snap in self.snapshots.items()
             if cid == case_id),
            key=lambda snap: snap["version"])
        current = versions[-1] if versions else None
        return {
            "case_id": case_id,
            # 冻结摘要：作业钉住的版本及其规范化内容摘要
            "frozen": (
                {"version": frozen_version,
                 "content_hash": self.snapshots[(case_id, frozen_version)]["content_hash"],
                 "released_at": self.snapshots[(case_id, frozen_version)]["released_at"].isoformat()}
                if frozen_version is not None
                and (case_id, frozen_version) in self.snapshots else None),
            # 当前有效版本：账内最高版本（勘误只发新版本，旧版永不覆盖）
            "current": (None if current is None else {
                "version": current["version"],
                "content_hash": current["content_hash"],
                "released_at": current["released_at"].isoformat(),
                "replaces": current["replaces"],
                "erratum_note": current.get("erratum_note", ""),
                "registered_by": current.get("registered_by"),
            }),
            "current_version": current["version"] if current else None,
            "versions": [
                {"version": snap["version"],
                 "content_hash": snap["content_hash"],
                 "released_at": snap["released_at"].isoformat(),
                 "replaces": snap["replaces"],
                 "erratum_note": snap.get("erratum_note", ""),
                 "registered_by": snap.get("registered_by"),
                 "status": snap.get("register_status", REGISTER_FIRST)}
                for snap in versions],
            "superseded": frozen_version is not None and current is not None
                          and frozen_version < current["version"],
            # 被拒绝的导入：异内容覆盖/重放不一致/乱序或成环替代/并发抢登
            "rejected_imports": [
                {"reason": entry["reason"], "at": entry["at"],
                 "submitted_by": entry["submitted_by"],
                 "version": entry["version"], "diff": entry["diff"],
                 "message": entry["message"]}
                for entry in self.rejected_imports
                if entry["case_id"] == case_id],
        }

    # ---- 查询 -----------------------------------------------------------

    def assignments_for_case(self, case_id: str) -> list[dict]:
        """列出依赖某病例的全部作业（跨课程），供撤回/勘误时清点影响面。"""
        result = []
        for assignment in self.assignments.values():
            task = self.tasks[assignment["task_id"]]
            if task["case_id"] == case_id:
                result.append({
                    "assignment_id": assignment["id"],
                    "course_id": task["course_id"],
                    "student_id": assignment["student_id"],
                    "status": assignment["status"],
                    "risk_flags": assignment["risk_flags"],
                })
        return result


def self_check(seed_path: str | Path) -> None:
    """装载种子并核对六账齐备、同病例跨课程关系成立。"""
    box = Sandbox.from_seed(seed_path)
    assert box.cases and box.snapshots and box.consents
    assert box.policies and box.environment_versions and box.assignments
    shared = next(iter(box.cases.values()))
    assert len(shared["courses"]) == 2, "种子中应有一个病例同时进入两门课程"
    print(f"领域检查通过：{len(box.cases)} 个病例，{len(box.snapshots)} 个快照版本，"
          f"{len(box.assignments)} 份作业，病例 {shared['id']} 进入 {shared['courses']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="医学教学数据沙箱领域规则")
    parser.add_argument("--check", action="store_true", help="装载种子夹具并自检")
    parser.add_argument("--seed", default="fixtures/seed.json")
    args = parser.parse_args()
    if args.check:
        self_check(args.seed)


if __name__ == "__main__":
    main()
