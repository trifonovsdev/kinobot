"""
User repository — all database operations for users & referrals.
"""

import datetime
import random
import string
from typing import Optional

from app.db.database import users_db
from app.core.logging import logger


class UserRepository:
    """Data access layer for users."""

    async def get_all(self) -> list[dict]:
        """Get all users ordered by newest first."""
        return await users_db.fetch_all("SELECT * FROM users ORDER BY id DESC")

    async def get_by_id(self, user_id: int) -> Optional[dict]:
        """Get user by internal DB id."""
        return await users_db.fetch_one(
            "SELECT * FROM users WHERE id = ?", (user_id,)
        )

    async def get_by_tg_id(self, tg_id: int) -> Optional[dict]:
        """Get user by Telegram user ID."""
        return await users_db.fetch_one(
            "SELECT * FROM users WHERE tg_id = ?", (tg_id,)
        )

    async def is_banned(self, tg_id: int) -> bool:
        """Check if user is banned."""
        row = await users_db.fetch_one(
            "SELECT banned FROM users WHERE tg_id = ?", (tg_id,)
        )
        return bool(row and row["banned"] == 1)

    async def is_admin(self, tg_id: int) -> bool:
        """Check if user is admin/traffer."""
        row = await users_db.fetch_one(
            "SELECT admin FROM users WHERE tg_id = ?", (tg_id,)
        )
        return bool(row and row["admin"] == 1)

    async def register(self, tg_id: int, name: str) -> str:
        """Register user if not exists. Returns referral code."""
        existing = await self.get_by_tg_id(tg_id)
        if existing:
            return existing.get("referral_code") or ""

        code = self._generate_referral_code()
        async with users_db.connection() as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (name, tg_id, admin, referral_code) VALUES (?, ?, 0, ?)",
                (name, tg_id, code),
            )
            await db.commit()
        return code

    async def process_referral(self, tg_id: int, referral_code: str) -> bool:
        """Process a referral link. Returns True if referral was recorded."""
        referral_code = referral_code.upper()
        user = await self.get_by_tg_id(tg_id)
        if not user:
            return False
        # Skip if already referred or self-referral
        if user.get("referred_by"):
            return False
        if user.get("referral_code") == referral_code:
            return False

        # Find referrer
        referrer = await users_db.fetch_one(
            "SELECT tg_id FROM users WHERE referral_code = ?", (referral_code,)
        )
        if not referrer:
            return False

        referrer_id = referrer["tg_id"]
        async with users_db.connection() as db:
            await db.execute(
                "UPDATE users SET referred_by = ? WHERE tg_id = ?",
                (referral_code, tg_id),
            )
            await db.execute(
                "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
                (referrer_id, tg_id),
            )
            await db.commit()
        return True

    async def toggle_admin(self, user_id: int) -> Optional[dict]:
        """Toggle admin status. Returns updated user info."""
        user = await self.get_by_id(user_id)
        if not user:
            return None
        new_status = 0 if user["admin"] else 1
        async with users_db.connection() as db:
            await db.execute(
                "UPDATE users SET admin = ? WHERE id = ?", (new_status, user_id)
            )
            await db.commit()
        return {"name": user["name"], "admin": new_status}

    async def toggle_ban(self, user_id: int) -> Optional[dict]:
        """Toggle ban status. Returns updated user info."""
        user = await self.get_by_id(user_id)
        if not user:
            return None
        new_status = 0 if user["banned"] else 1
        async with users_db.connection() as db:
            await db.execute(
                "UPDATE users SET banned = ? WHERE id = ?", (new_status, user_id)
            )
            await db.commit()
        return {"name": user["name"], "banned": new_status}

    async def get_referral_stats(self, tg_id: int) -> dict:
        """Get referral statistics for a user."""
        user = await self.get_by_tg_id(tg_id)
        if not user:
            return {"code": "", "total": 0, "recent": []}

        code = user.get("referral_code") or ""
        if not code:
            code = self._generate_referral_code()
            async with users_db.connection() as db:
                await db.execute(
                    "UPDATE users SET referral_code = ? WHERE tg_id = ?", (code, tg_id)
                )
                await db.commit()

        total_row = await users_db.fetch_one(
            "SELECT COUNT(*) as cnt FROM referrals WHERE referrer_id = ?", (tg_id,)
        )
        total = total_row["cnt"] if total_row else 0

        recent = await users_db.fetch_all(
            """
            SELECT u.tg_id, u.name, r.date_referred
            FROM referrals r
            JOIN users u ON u.tg_id = r.referred_id
            WHERE r.referrer_id = ?
            ORDER BY r.date_referred DESC
            LIMIT 10
            """,
            (tg_id,),
        )

        return {"code": code, "total": total, "recent": recent}

    async def get_stats(self) -> dict:
        """Get user statistics."""
        async with users_db.connection() as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE admin = 1")
            admins = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
            banned = (await cursor.fetchone())[0]

            # Referrals per day (last 7 days)
            cursor = await db.execute(
                "SELECT date(date_referred) as d, COUNT(*) as c "
                "FROM referrals WHERE date_referred >= date('now','-6 day') "
                "GROUP BY d ORDER BY d"
            )
            raw = {row[0]: row[1] for row in await cursor.fetchall()}

        today = datetime.date.today()
        last7 = [(today - datetime.timedelta(days=i)).isoformat() for i in range(6, -1, -1)]
        referrals = {
            "labels": last7,
            "counts": [raw.get(day, 0) for day in last7],
        }

        return {
            "total": total,
            "admins": admins,
            "banned": banned,
            "referrals": referrals,
        }

    @staticmethod
    def _generate_referral_code() -> str:
        """Generate a 6-character referral code."""
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


# Singleton
user_repository = UserRepository()
