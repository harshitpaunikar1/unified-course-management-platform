"""
Unified course management platform service layer.
Handles enrolment, assessment grading, certification, cohort analytics, and at-risk alerts.
"""
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from models import (
        PlatformDB, User, Course, Module, Enrolment, Assessment,
        AssessmentSubmission, AttendanceRecord, Certificate,
        EnrolmentStatus, AssessmentType, UserRole, ContentType,
    )
    MODELS_AVAILABLE = True
except ImportError:
    MODELS_AVAILABLE = False


class EnrolmentManager:
    """Manages enrolments, waitlists, and capacity checks."""

    def __init__(self, db: "PlatformDB"):
        self.db = db

    def enrol_learner(self, user_id: str, course_id: str,
                       payment_ref: Optional[str] = None) -> Dict[str, Any]:
        existing = self.db.conn.execute(
            "SELECT enrolment_id, status FROM enrolments WHERE user_id=? AND course_id=?",
            (user_id, course_id),
        ).fetchone()
        if existing:
            return {"success": False, "message": "Already enrolled.", "status": existing[1]}

        course_row = self.db.conn.execute(
            "SELECT max_enrolments FROM courses WHERE course_id=?", (course_id,)
        ).fetchone()
        if not course_row:
            return {"success": False, "message": "Course not found."}

        current = self.db.conn.execute(
            "SELECT COUNT(*) FROM enrolments WHERE course_id=? AND status NOT IN ('dropped','waitlisted')",
            (course_id,),
        ).fetchone()[0]

        if current >= course_row[0]:
            status = EnrolmentStatus.WAITLISTED
        else:
            status = EnrolmentStatus.ENROLLED

        enrolment_id = hashlib.md5(f"{user_id}{course_id}{time.time()}".encode()).hexdigest()[:16]
        enrolment = Enrolment(enrolment_id, user_id, course_id, status,
                               payment_ref=payment_ref)
        self.db.enrol(enrolment)
        logger.info("Enrolled user=%s course=%s status=%s", user_id, course_id, status.value)
        return {"success": True, "enrolment_id": enrolment_id, "status": status.value}

    def drop_enrolment(self, user_id: str, course_id: str) -> bool:
        self.db.conn.execute(
            "UPDATE enrolments SET status=? WHERE user_id=? AND course_id=?",
            (EnrolmentStatus.DROPPED.value, user_id, course_id),
        )
        self.db.conn.commit()
        self._promote_from_waitlist(course_id)
        return True

    def _promote_from_waitlist(self, course_id: str) -> None:
        waitlisted = self.db.conn.execute(
            "SELECT enrolment_id FROM enrolments WHERE course_id=? AND status=? ORDER BY enrolled_at LIMIT 1",
            (course_id, EnrolmentStatus.WAITLISTED.value),
        ).fetchone()
        if waitlisted:
            self.db.conn.execute(
                "UPDATE enrolments SET status=? WHERE enrolment_id=?",
                (EnrolmentStatus.ENROLLED.value, waitlisted[0]),
            )
            self.db.conn.commit()
            logger.info("Promoted waitlisted enrolment %s to enrolled.", waitlisted[0])

    def update_progress(self, user_id: str, course_id: str, progress_pct: float) -> None:
        clamped = max(0.0, min(1.0, progress_pct))
        status = EnrolmentStatus.COMPLETED.value if clamped >= 1.0 else EnrolmentStatus.IN_PROGRESS.value
        completed_at = time.time() if clamped >= 1.0 else None
        self.db.conn.execute(
            "UPDATE enrolments SET progress_pct=?, status=?, completed_at=? WHERE user_id=? AND course_id=?",
            (clamped, status, completed_at, user_id, course_id),
        )
        self.db.conn.commit()


class AssessmentEngine:
    """Handles assessment submission, auto-grading, and pass/fail determination."""

    def __init__(self, db: "PlatformDB"):
        self.db = db

    def submit(self, user_id: str, assessment_id: str, score: float,
                feedback: str = "") -> Dict[str, Any]:
        assessment_row = self.db.conn.execute(
            "SELECT max_score, passing_score, course_id FROM assessments WHERE assessment_id=?",
            (assessment_id,),
        ).fetchone()
        if not assessment_row:
            return {"success": False, "message": "Assessment not found."}
        max_score, passing_score, course_id = assessment_row
        clamped_score = max(0.0, min(float(score), float(max_score)))
        passed = clamped_score >= float(passing_score)
        submission_id = hashlib.md5(f"{user_id}{assessment_id}{time.time()}".encode()).hexdigest()[:16]
        sub = AssessmentSubmission(
            submission_id=submission_id,
            assessment_id=assessment_id,
            user_id=user_id,
            score=clamped_score,
            feedback=feedback,
            graded_at=time.time(),
        )
        self.db.submit_assessment(sub)
        logger.info("Assessment %s submitted by %s: score=%.1f passed=%s",
                    assessment_id, user_id, clamped_score, passed)
        return {
            "submission_id": submission_id,
            "score": clamped_score,
            "max_score": max_score,
            "passed": passed,
        }

    def gradebook(self, course_id: str) -> pd.DataFrame:
        return pd.read_sql_query(
            """SELECT s.user_id, u.name, a.title AS assessment,
                      s.score, a.max_score, a.passing_score,
                      CASE WHEN s.score >= a.passing_score THEN 1 ELSE 0 END AS passed
               FROM submissions s
               JOIN assessments a ON s.assessment_id=a.assessment_id
               JOIN users u ON s.user_id=u.user_id
               WHERE a.course_id=?
               ORDER BY u.name, a.title""",
            self.db.conn, params=(course_id,),
        )


class CertificationService:
    """Issues certificates upon course completion."""

    GRADE_THRESHOLDS = [(90, "A+"), (80, "A"), (70, "B"), (60, "C"), (0, "F")]

    def __init__(self, db: "PlatformDB"):
        self.db = db

    def _compute_grade(self, score_pct: float) -> str:
        for threshold, grade in self.GRADE_THRESHOLDS:
            if score_pct >= threshold:
                return grade
        return "F"

    def issue_if_eligible(self, user_id: str, course_id: str) -> Optional[Dict[str, Any]]:
        enrolment = self.db.conn.execute(
            "SELECT status FROM enrolments WHERE user_id=? AND course_id=?",
            (user_id, course_id),
        ).fetchone()
        if not enrolment or enrolment[0] != EnrolmentStatus.COMPLETED.value:
            return None

        existing = self.db.conn.execute(
            "SELECT certificate_id FROM certificates WHERE user_id=? AND course_id=?",
            (user_id, course_id),
        ).fetchone()
        if existing:
            return {"certificate_id": existing[0], "already_issued": True}

        avg_score = self.db.conn.execute(
            """SELECT AVG(s.score / a.max_score * 100)
               FROM submissions s
               JOIN assessments a ON s.assessment_id=a.assessment_id
               WHERE s.user_id=? AND a.course_id=?""",
            (user_id, course_id),
        ).fetchone()[0] or 0.0

        grade = self._compute_grade(float(avg_score))
        cert_id = hashlib.md5(f"{user_id}{course_id}{time.time()}".encode()).hexdigest()[:16]
        verification_code = hashlib.sha256(cert_id.encode()).hexdigest()[:20].upper()
        cert = Certificate(
            certificate_id=cert_id,
            user_id=user_id,
            course_id=course_id,
            issued_at=time.time(),
            certificate_url=f"https://certs.platform.internal/{cert_id}",
            grade=grade,
            score=round(float(avg_score), 2),
            verification_code=verification_code,
        )
        self.db.issue_certificate(cert)
        logger.info("Certificate issued: user=%s course=%s grade=%s", user_id, course_id, grade)
        return {"certificate_id": cert_id, "grade": grade, "score": round(float(avg_score), 2),
                "verification_code": verification_code}


class CohortAnalytics:
    """Cohort-level engagement, completion, and at-risk analysis."""

    def __init__(self, db: "PlatformDB"):
        self.db = db

    def completion_funnel(self, course_id: str) -> Dict[str, int]:
        rows = self.db.conn.execute(
            "SELECT status, COUNT(*) FROM enrolments WHERE course_id=? GROUP BY status",
            (course_id,),
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    def engagement_trend(self, course_id: str) -> pd.DataFrame:
        return pd.read_sql_query(
            """SELECT DATE(enrolled_at, 'unixepoch') AS enrol_date,
                      COUNT(*) AS new_enrolments,
                      AVG(progress_pct) AS avg_progress
               FROM enrolments
               WHERE course_id=?
               GROUP BY enrol_date ORDER BY enrol_date""",
            self.db.conn, params=(course_id,),
        )

    def instructor_dashboard(self, instructor_id: str) -> Dict[str, Any]:
        courses = self.db.conn.execute(
            "SELECT course_id, title FROM courses WHERE instructor_id=?",
            (instructor_id,),
        ).fetchall()
        summary = []
        for course_id, title in courses:
            enrolments = self.db.conn.execute(
                "SELECT COUNT(*), AVG(progress_pct) FROM enrolments WHERE course_id=?",
                (course_id,),
            ).fetchone()
            summary.append({
                "course_id": course_id,
                "title": title,
                "total_enrolments": enrolments[0] or 0,
                "avg_progress": round(float(enrolments[1] or 0), 3),
            })
        return {"instructor_id": instructor_id, "courses": summary}


class CoursePlatformService:
    """Facade combining all platform capabilities."""

    def __init__(self, db_path: str = ":memory:"):
        if not MODELS_AVAILABLE:
            raise RuntimeError("models.py required.")
        self.db = PlatformDB(db_path=db_path)
        self.enrolments = EnrolmentManager(self.db)
        self.assessments = AssessmentEngine(self.db)
        self.certifications = CertificationService(self.db)
        self.cohort = CohortAnalytics(self.db)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if not MODELS_AVAILABLE:
        print("models.py not found.")
    else:
        svc = CoursePlatformService()

        instructor = User("inst_001", "Dr. Kumar", "kumar@uni.edu", UserRole.INSTRUCTOR)
        svc.db.upsert_user(instructor)
        svc.db.upsert_course(Course("crs_001", "Data Science Bootcamp",
                                     "Comprehensive data science course", "inst_001",
                                     "Data Science", 30, "2024-02-01", "2024-05-31",
                                     price_inr=8999.0, is_published=True))
        svc.db.conn.execute(
            "INSERT OR IGNORE INTO assessments VALUES (?,?,?,?,?,?,?,?)",
            ("asmnt_001", "crs_001", None, "Final Exam", "quiz", 100, 60, "2024-05-25"),
        )
        svc.db.conn.commit()

        rng = np.random.default_rng(42)
        learner_ids = [f"learn_{i:03d}" for i in range(8)]
        for lid in learner_ids:
            u = User(lid, f"Student {lid}", f"{lid}@example.com", UserRole.LEARNER)
            svc.db.upsert_user(u)
            result = svc.enrolments.enrol_learner(lid, "crs_001")
            print(f"Enrolled {lid}: {result['status']}")
            svc.enrolments.update_progress(lid, "crs_001", float(rng.uniform(0.2, 1.0)))
            score = float(rng.uniform(40, 100))
            svc.assessments.submit(lid, "asmnt_001", score)
            if rng.uniform() > 0.5:
                svc.enrolments.update_progress(lid, "crs_001", 1.0)
                cert_result = svc.certifications.issue_if_eligible(lid, "crs_001")
                if cert_result:
                    print(f"  Certificate for {lid}: grade={cert_result.get('grade')}")

        print("\nCompletion funnel:")
        print(svc.cohort.completion_funnel("crs_001"))

        print("\nGradebook (first 5):")
        gb = svc.assessments.gradebook("crs_001")
        if not gb.empty:
            print(gb.head(5).to_string(index=False))

        print("\nAt-risk learners:")
        at_risk = svc.db.at_risk_learners(min_progress=0.40, min_days_enrolled=0)
        print(f"  {len(at_risk)} at-risk learners identified")

        print("\nInstructor dashboard:")
        dash = svc.cohort.instructor_dashboard("inst_001")
        for c in dash["courses"]:
            print(f"  {c['title']}: {c['total_enrolments']} enrolments, "
                  f"avg progress {c['avg_progress']:.0%}")
