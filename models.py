"""
Data models for the unified course management platform.
Covers courses, modules, enrolments, assessments, attendance, and certifications.
"""
import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class EnrolmentStatus(str, Enum):
    WAITLISTED = "waitlisted"
    ENROLLED = "enrolled"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    DROPPED = "dropped"


class AssessmentType(str, Enum):
    QUIZ = "quiz"
    ASSIGNMENT = "assignment"
    PROJECT = "project"
    PEER_REVIEW = "peer_review"


class UserRole(str, Enum):
    ADMIN = "admin"
    INSTRUCTOR = "instructor"
    LEARNER = "learner"
    COORDINATOR = "coordinator"


class ContentType(str, Enum):
    VIDEO = "video"
    PDF = "pdf"
    QUIZ = "quiz"
    LIVE_SESSION = "live_session"
    ASSIGNMENT = "assignment"


@dataclass
class User:
    user_id: str
    name: str
    email: str
    role: UserRole
    created_at: float = field(default_factory=time.time)
    is_active: bool = True


@dataclass
class Course:
    course_id: str
    title: str
    description: str
    instructor_id: str
    category: str
    max_enrolments: int
    start_date: str
    end_date: str
    price_inr: float = 0.0
    is_published: bool = False
    created_at: float = field(default_factory=time.time)


@dataclass
class Module:
    module_id: str
    course_id: str
    title: str
    content_type: ContentType
    order_index: int
    duration_minutes: int
    content_url: str = ""
    is_mandatory: bool = True


@dataclass
class Enrolment:
    enrolment_id: str
    user_id: str
    course_id: str
    status: EnrolmentStatus
    enrolled_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    progress_pct: float = 0.0
    payment_ref: Optional[str] = None


@dataclass
class Assessment:
    assessment_id: str
    course_id: str
    module_id: Optional[str]
    title: str
    assessment_type: AssessmentType
    max_score: float
    passing_score: float
    due_date: Optional[str] = None


@dataclass
class AssessmentSubmission:
    submission_id: str
    assessment_id: str
    user_id: str
    score: float
    feedback: str = ""
    submitted_at: float = field(default_factory=time.time)
    graded_at: Optional[float] = None

    @property
    def passed(self) -> bool:
        return False


@dataclass
class AttendanceRecord:
    record_id: str
    user_id: str
    module_id: str
    course_id: str
    attended: bool
    session_date: str
    duration_minutes: int = 0


@dataclass
class Certificate:
    certificate_id: str
    user_id: str
    course_id: str
    issued_at: float
    certificate_url: str
    grade: str
    score: float
    verification_code: str = ""


class PlatformDB:
    """SQLite-backed persistence for the course management platform."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY, name TEXT, email TEXT UNIQUE,
        role TEXT, created_at REAL, is_active INTEGER
    );
    CREATE TABLE IF NOT EXISTS courses (
        course_id TEXT PRIMARY KEY, title TEXT, description TEXT,
        instructor_id TEXT, category TEXT, max_enrolments INTEGER,
        start_date TEXT, end_date TEXT, price_inr REAL,
        is_published INTEGER, created_at REAL
    );
    CREATE TABLE IF NOT EXISTS modules (
        module_id TEXT PRIMARY KEY, course_id TEXT, title TEXT,
        content_type TEXT, order_index INTEGER, duration_minutes INTEGER,
        content_url TEXT, is_mandatory INTEGER
    );
    CREATE TABLE IF NOT EXISTS enrolments (
        enrolment_id TEXT PRIMARY KEY, user_id TEXT, course_id TEXT,
        status TEXT, enrolled_at REAL, completed_at REAL,
        progress_pct REAL, payment_ref TEXT
    );
    CREATE TABLE IF NOT EXISTS assessments (
        assessment_id TEXT PRIMARY KEY, course_id TEXT, module_id TEXT,
        title TEXT, assessment_type TEXT, max_score REAL,
        passing_score REAL, due_date TEXT
    );
    CREATE TABLE IF NOT EXISTS submissions (
        submission_id TEXT PRIMARY KEY, assessment_id TEXT, user_id TEXT,
        score REAL, feedback TEXT, submitted_at REAL, graded_at REAL
    );
    CREATE TABLE IF NOT EXISTS attendance (
        record_id TEXT PRIMARY KEY, user_id TEXT, module_id TEXT,
        course_id TEXT, attended INTEGER, session_date TEXT, duration_minutes INTEGER
    );
    CREATE TABLE IF NOT EXISTS certificates (
        certificate_id TEXT PRIMARY KEY, user_id TEXT, course_id TEXT,
        issued_at REAL, certificate_url TEXT, grade TEXT,
        score REAL, verification_code TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_enrolments_user ON enrolments(user_id);
    CREATE INDEX IF NOT EXISTS idx_enrolments_course ON enrolments(course_id);
    CREATE INDEX IF NOT EXISTS idx_submissions_user ON submissions(user_id);
    CREATE INDEX IF NOT EXISTS idx_attendance_course ON attendance(course_id);
    """

    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(self.SCHEMA)
        self.conn.commit()

    def upsert_user(self, u: User) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?)",
            (u.user_id, u.name, u.email, u.role.value, u.created_at, int(u.is_active)),
        )
        self.conn.commit()

    def upsert_course(self, c: Course) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO courses VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (c.course_id, c.title, c.description, c.instructor_id,
             c.category, c.max_enrolments, c.start_date, c.end_date,
             c.price_inr, int(c.is_published), c.created_at),
        )
        self.conn.commit()

    def upsert_module(self, m: Module) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO modules VALUES (?,?,?,?,?,?,?,?)",
            (m.module_id, m.course_id, m.title, m.content_type.value,
             m.order_index, m.duration_minutes, m.content_url, int(m.is_mandatory)),
        )
        self.conn.commit()

    def enrol(self, e: Enrolment) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO enrolments VALUES (?,?,?,?,?,?,?,?)",
            (e.enrolment_id, e.user_id, e.course_id, e.status.value,
             e.enrolled_at, e.completed_at, e.progress_pct, e.payment_ref),
        )
        self.conn.commit()

    def submit_assessment(self, s: AssessmentSubmission) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO submissions VALUES (?,?,?,?,?,?,?)",
            (s.submission_id, s.assessment_id, s.user_id, s.score,
             s.feedback, s.submitted_at, s.graded_at),
        )
        self.conn.commit()

    def issue_certificate(self, cert: Certificate) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO certificates VALUES (?,?,?,?,?,?,?,?)",
            (cert.certificate_id, cert.user_id, cert.course_id,
             cert.issued_at, cert.certificate_url, cert.grade,
             cert.score, cert.verification_code),
        )
        self.conn.commit()

    def get_course_enrolments(self, course_id: str) -> List[Dict]:
        cur = self.conn.execute(
            "SELECT * FROM enrolments WHERE course_id=?", (course_id,)
        )
        return [dict(row) for row in cur.fetchall()]

    def learner_progress(self, user_id: str) -> List[Dict]:
        import pandas as pd
        return pd.read_sql_query(
            """SELECT e.course_id, c.title, e.status, e.progress_pct,
                      e.enrolled_at, e.completed_at
               FROM enrolments e JOIN courses c ON e.course_id=c.course_id
               WHERE e.user_id=?""",
            self.conn, params=(user_id,),
        ).to_dict(orient="records")

    def at_risk_learners(self, min_progress: float = 0.20,
                          min_days_enrolled: int = 14) -> List[Dict]:
        cutoff = time.time() - min_days_enrolled * 86400
        import pandas as pd
        return pd.read_sql_query(
            f"""SELECT e.user_id, u.name, e.course_id, e.progress_pct, e.enrolled_at
                FROM enrolments e JOIN users u ON e.user_id=u.user_id
                WHERE e.status='in_progress'
                  AND e.enrolled_at < {cutoff}
                  AND e.progress_pct < {min_progress}""",
            self.conn,
        ).to_dict(orient="records")

    def course_analytics(self, course_id: str) -> Dict[str, Any]:
        import pandas as pd
        enrol_df = pd.read_sql_query(
            "SELECT status, COUNT(*) AS cnt FROM enrolments WHERE course_id=? GROUP BY status",
            self.conn, params=(course_id,),
        )
        sub_df = pd.read_sql_query(
            """SELECT AVG(s.score) AS avg_score, COUNT(*) AS submissions
               FROM submissions s
               JOIN assessments a ON s.assessment_id=a.assessment_id
               WHERE a.course_id=?""",
            self.conn, params=(course_id,),
        )
        return {
            "enrolments_by_status": enrol_df.set_index("status")["cnt"].to_dict(),
            "avg_assessment_score": round(float(sub_df["avg_score"].iloc[0] or 0), 2),
            "total_submissions": int(sub_df["submissions"].iloc[0] or 0),
        }


if __name__ == "__main__":
    import numpy as np

    db = PlatformDB()

    instructor = User("inst_001", "Dr. Meera Sharma", "meera@uni.edu",
                       UserRole.INSTRUCTOR)
    learners = [User(f"learn_{i:03d}", f"Learner {i}", f"learner{i}@edu.in",
                      UserRole.LEARNER) for i in range(10)]
    for u in [instructor] + learners:
        db.upsert_user(u)

    course = Course("crs_001", "Machine Learning Fundamentals",
                     "Intro to ML algorithms and applications",
                     "inst_001", "Data Science", 50,
                     "2024-01-15", "2024-03-31", price_inr=4999.0, is_published=True)
    db.upsert_course(course)

    for i, ct in enumerate([ContentType.VIDEO, ContentType.PDF, ContentType.QUIZ,
                              ContentType.LIVE_SESSION, ContentType.ASSIGNMENT]):
        db.upsert_module(Module(f"mod_{i:03d}", "crs_001", f"Module {i+1}",
                                 ct, i, 45))

    rng = np.random.default_rng(42)
    for learner in learners:
        enrolment_id = f"enr_{learner.user_id}"
        db.enrol(Enrolment(enrolment_id, learner.user_id, "crs_001",
                            EnrolmentStatus.IN_PROGRESS,
                            progress_pct=float(rng.uniform(0, 1.0))))

    print("Course analytics:")
    analytics = db.course_analytics("crs_001")
    print(f"  Enrolments: {analytics['enrolments_by_status']}")
    print(f"  Avg score: {analytics['avg_assessment_score']}")

    print(f"\nLearner progress for learn_000:")
    progress = db.learner_progress("learn_000")
    for p in progress:
        print(f"  {p['title']}: {p['status']} ({p['progress_pct']:.0%})")
