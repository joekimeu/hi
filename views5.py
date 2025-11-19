# views4.py

from flask import request, jsonify
from app import app  # adjust if your app import is different
import pyodbc
from flask import current_app


def get_db_connection():
    """
    Returns a new DB connection.
    Adjust this to match how you already connect to SQL Server.
    For example, you may already have a shared connection or helper.
    """
    conn_str = current_app.config['DB_CONNECTION_STRING']
    return pyodbc.connect(conn_str)


def _get_feedback_summary(cursor, incident_number, racf=None):
    """
    Helper: returns (likes, dislikes, user_feedback)
    - likes: int
    - dislikes: int
    - user_feedback: 'like' | 'dislike' | None
    """
    # Total counts per incident
    cursor.execute(
        """
        SELECT feedback_type, COUNT(*) AS cnt
        FROM dbo.feedback
        WHERE incident_number = ?
        GROUP BY feedback_type
        """,
        (incident_number,)
    )

    likes = 0
    dislikes = 0
    for feedback_type, cnt in cursor.fetchall():
        if feedback_type.lower() == "like":
            likes = cnt
        elif feedback_type.lower() == "dislike":
            dislikes = cnt

    user_feedback = None
    if racf:
        cursor.execute(
            """
            SELECT TOP 1 feedback_type
            FROM dbo.feedback
            WHERE incident_number = ? AND racf = ?
            ORDER BY id DESC
            """,
            (incident_number, racf)
        )
        row = cursor.fetchone()
        if row:
            user_feedback = row[0].lower()

    return likes, dislikes, user_feedback


@app.route("/feedback/status", methods=["POST"])
def feedback_status():
    """
    Returns current feedback status for an incident + user.
    Payload JSON: { "racf": "...", "similarincident_number": "..." }
    Response JSON:
      {
        "success": true,
        "likes": 10,
        "dislikes": 2,
        "user_feedback": "like" | "dislike" | null,
        "can_cancel": true/false
      }
    """
    data = request.get_json() or {}
    racf = data.get("racf")
    incident_number = data.get("similarincident_number")

    if not incident_number:
        return jsonify({"success": False, "error": "incident_number required"}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        likes, dislikes, user_feedback = _get_feedback_summary(cursor, incident_number, racf)
    finally:
        conn.close()

    return jsonify({
        "success": True,
        "likes": likes,
        "dislikes": dislikes,
        "user_feedback": user_feedback,
        "can_cancel": user_feedback is not None,
    })


@app.route("/feedback/like", methods=["POST"])
def feedback_like():
    """
    User clicks Like.
    Rules:
    - If no existing feedback: insert 'like'
    - If existing 'dislike': switch to 'like'
    - If already 'like': no-op, just return counts
    """
    data = request.get_json() or {}
    racf = data.get("racf")
    incident_number = data.get("similarincident_number")

    if not incident_number or not racf:
        return jsonify({"success": False, "error": "racf and incident_number required"}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        # Check existing feedback for this user/incident
        cursor.execute(
            """
            SELECT TOP 1 id, feedback_type
            FROM dbo.feedback
            WHERE incident_number = ? AND racf = ?
            ORDER BY id DESC
            """,
            (incident_number, racf)
        )
        row = cursor.fetchone()

        if row:
            feedback_id, current_type = row
            current_type = current_type.lower()
            if current_type == "like":
                # Already liked: do nothing
                pass
            else:
                # Switch from dislike -> like
                cursor.execute(
                    """
                    UPDATE dbo.feedback
                    SET feedback_type = 'like'
                    WHERE id = ?
                    """,
                    (feedback_id,)
                )
        else:
            # No existing feedback: insert like
            cursor.execute(
                """
                INSERT INTO dbo.feedback (incident_number, racf, feedback_type)
                VALUES (?, ?, 'like')
                """,
                (incident_number, racf)
            )

        conn.commit()

        likes, dislikes, user_feedback = _get_feedback_summary(cursor, incident_number, racf)

    finally:
        conn.close()

    return jsonify({
        "success": True,
        "likes": likes,
        "dislikes": dislikes,
        "user_feedback": user_feedback,
        "can_cancel": user_feedback is not None,
    })


@app.route("/feedback/dislike", methods=["POST"])
def feedback_dislike():
    """
    User clicks Dislike.
    Rules:
    - If no existing feedback: insert 'dislike'
    - If existing 'like': switch to 'dislike'
    - If already 'dislike': no-op, just return counts
    """
    data = request.get_json() or {}
    racf = data.get("racf")
    incident_number = data.get("similarincident_number")

    if not incident_number or not racf:
        return jsonify({"success": False, "error": "racf and incident_number required"}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT TOP 1 id, feedback_type
            FROM dbo.feedback
            WHERE incident_number = ? AND racf = ?
            ORDER BY id DESC
            """,
            (incident_number, racf)
        )
        row = cursor.fetchone()

        if row:
            feedback_id, current_type = row
            current_type = current_type.lower()
            if current_type == "dislike":
                # Already disliked: do nothing
                pass
            else:
                # Switch from like -> dislike
                cursor.execute(
                    """
                    UPDATE dbo.feedback
                    SET feedback_type = 'dislike'
                    WHERE id = ?
                    """,
                    (feedback_id,)
                )
        else:
            # No existing feedback: insert dislike
            cursor.execute(
                """
                INSERT INTO dbo.feedback (incident_number, racf, feedback_type)
                VALUES (?, ?, 'dislike')
                """,
                (incident_number, racf)
            )

        conn.commit()
        likes, dislikes, user_feedback = _get_feedback_summary(cursor, incident_number, racf)

    finally:
        conn.close()

    return jsonify({
        "success": True,
        "likes": likes,
        "dislikes": dislikes,
        "user_feedback": user_feedback,
        "can_cancel": user_feedback is not None,
    })


@app.route("/feedback/cancel", methods=["POST"])
def feedback_cancel():
    """
    User clicks 'Cancel my feedback'.
    Deletes their feedback row for this incident.
    """
    data = request.get_json() or {}
    racf = data.get("racf")
    incident_number = data.get("similarincident_number")

    if not incident_number or not racf:
        return jsonify({"success": False, "error": "racf and incident_number required"}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            DELETE FROM dbo.feedback
            WHERE incident_number = ? AND racf = ?
            """,
            (incident_number, racf)
        )
        conn.commit()

        likes, dislikes, user_feedback = _get_feedback_summary(cursor, incident_number, racf)

    finally:
        conn.close()

    return jsonify({
        "success": True,
        "likes": likes,
        "dislikes": dislikes,
        "user_feedback": user_feedback,  # should now be None
        "can_cancel": False,
    })
