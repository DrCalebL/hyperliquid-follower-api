"""
Trade Reconciliation Script (Hyperliquid)
==========================================
Reads actual closed trades from Hyperliquid and backfills them 
into portfolio_trades table for accurate P&L tracking.

Usage:
    python trade_reconciliation.py
    python trade_reconciliation.py <user_id>

Requires:
    - DATABASE_URL environment variable
    - User credentials in follower_users table
"""

import asyncio
import asyncpg
import os
from datetime import datetime, timedelta
from cryptography.fernet import Fernet

from hyperliquid.info import Info
from hyperliquid.utils import constants
from eth_account import Account

DATABASE_URL = os.getenv("DATABASE_URL")
CREDENTIALS_ENCRYPTION_KEY = os.getenv("CREDENTIALS_ENCRYPTION_KEY")
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"


def decrypt_credential(encrypted_value: str) -> str:
    """Decrypt a stored credential"""
    if not CREDENTIALS_ENCRYPTION_KEY or not encrypted_value:
        return ""
    try:
        f = Fernet(CREDENTIALS_ENCRYPTION_KEY.encode() if isinstance(CREDENTIALS_ENCRYPTION_KEY, str) else CREDENTIALS_ENCRYPTION_KEY)
        return f.decrypt(encrypted_value.encode()).decode()
    except Exception as e:
        print(f"Decryption error: {e}")
        return ""


def get_info():
    """Get Hyperliquid Info instance"""
    base_url = constants.TESTNET_API_URL if USE_TESTNET else constants.MAINNET_API_URL
    return Info(base_url, skip_ws=True)


async def get_hl_closed_trades(wallet_address: str, since_days: int = 30):
    """
    Fetch closed trades from Hyperliquid using user_fills.
    Returns list of round-trip trades with P&L.
    """
    try:
        info = get_info()
        since_ts = int((datetime.utcnow() - timedelta(days=since_days)).timestamp() * 1000)
        
        all_fills = info.user_fills(wallet_address)
        recent_fills = [f for f in all_fills if f.get('time', 0) >= since_ts]
        
        print(f"📊 Fetched {len(recent_fills)} fills from Hyperliquid (last {since_days} days)")
        
        round_trips = []
        positions = {}
        
        for fill in sorted(recent_fills, key=lambda f: f.get('time', 0)):
            coin = fill.get('coin', '')
            side = fill.get('side', '').lower()
            if side in ('b', 'buy', 'long'):
                side = 'buy'
            elif side in ('a', 'sell', 'short'):
                side = 'sell'
            
            amount = float(fill.get('sz', 0))
            price = float(fill.get('px', 0))
            timestamp = fill.get('time', 0)
            fee = abs(float(fill.get('fee', 0)))
            
            if not coin or amount == 0:
                continue
            
            if coin not in positions:
                positions[coin] = {'entries': [], 'side': None, 'total_amount': 0, 'avg_entry': 0}
            
            pos = positions[coin]
            
            is_opening = (
                pos['total_amount'] == 0 or
                (pos['side'] == 'long' and side == 'buy') or
                (pos['side'] == 'short' and side == 'sell')
            )
            
            if is_opening:
                if pos['total_amount'] == 0:
                    pos['side'] = 'long' if side == 'buy' else 'short'
                total_cost = pos['avg_entry'] * pos['total_amount'] + price * amount
                pos['total_amount'] += amount
                pos['avg_entry'] = total_cost / pos['total_amount'] if pos['total_amount'] > 0 else 0
                pos['entries'].append({'timestamp': timestamp, 'price': price, 'amount': amount, 'side': side})
                print(f"  📈 OPEN {pos['side'].upper()} {coin}: {amount} @ ${price:.5f}")
            else:
                close_amount = min(amount, pos['total_amount'])
                entry_price = pos['avg_entry']
                exit_price = price
                
                if pos['side'] == 'long':
                    pnl = (exit_price - entry_price) * close_amount
                else:
                    pnl = (entry_price - exit_price) * close_amount
                
                pnl_pct = ((exit_price / entry_price) - 1) * 100 if pos['side'] == 'long' else ((entry_price / exit_price) - 1) * 100
                
                round_trips.append({
                    'symbol': coin, 'side': pos['side'],
                    'entry_price': entry_price, 'exit_price': exit_price,
                    'quantity': close_amount, 'pnl_usd': pnl, 'pnl_pct': pnl_pct,
                    'entry_time': pos['entries'][0]['timestamp'] if pos['entries'] else timestamp,
                    'exit_time': timestamp, 'fee': fee
                })
                
                status = "🟢 WIN" if pnl > 0 else "🔴 LOSS"
                print(f"  {status} CLOSE {pos['side'].upper()} {coin}: {close_amount} @ ${exit_price:.5f} | P&L: ${pnl:.2f} ({pnl_pct:.2f}%)")
                
                pos['total_amount'] -= close_amount
                if pos['total_amount'] <= 0:
                    positions[coin] = {'entries': [], 'side': None, 'total_amount': 0, 'avg_entry': 0}
        
        return round_trips
        
    except Exception as e:
        print(f"❌ Error fetching HL trades: {e}")
        import traceback
        traceback.print_exc()
        return []


async def backfill_trades(conn, user_id: int, round_trips: list, fee_tier: str = 'standard'):
    """Insert round-trip trades into trades table"""
    from config import FEE_TIERS
    
    tier_config = FEE_TIERS.get(fee_tier, FEE_TIERS['standard'])
    fee_rate = tier_config['rate']
    
    inserted = 0
    total_pnl = 0
    total_fees = 0
    
    for rt in round_trips:
        try:
            position_size = rt['quantity'] * rt['exit_price']
            fee_amount = position_size * fee_rate if rt['pnl_usd'] > 0 else 0
            
            entry_time = datetime.utcfromtimestamp(rt['entry_time'] / 1000) if rt['entry_time'] > 1e10 else datetime.utcfromtimestamp(rt['entry_time'])
            exit_time = datetime.utcfromtimestamp(rt['exit_time'] / 1000) if rt['exit_time'] > 1e10 else datetime.utcfromtimestamp(rt['exit_time'])
            
            exists = await conn.fetchval("""
                SELECT COUNT(*) FROM trades 
                WHERE user_id = $1 AND symbol = $2
                AND ABS(EXTRACT(EPOCH FROM (opened_at - $3::timestamp))) < 60
                AND ABS(EXTRACT(EPOCH FROM (closed_at - $4::timestamp))) < 60
            """, user_id, rt['symbol'], entry_time, exit_time)
            
            if exists > 0:
                continue
            
            await conn.execute("""
                INSERT INTO trades (
                    user_id, symbol, hl_coin, side, 
                    entry_price, exit_price, quantity, leverage,
                    profit_usd, profit_percent, fee_usd,
                    opened_at, closed_at, source
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            """, user_id, rt['symbol'], rt['symbol'], rt['side'],
                rt['entry_price'], rt['exit_price'], rt['quantity'], 1,
                rt['pnl_usd'], rt['pnl_pct'], fee_amount,
                entry_time, exit_time, 'reconciliation')
            
            inserted += 1
            total_pnl += rt['pnl_usd']
            total_fees += fee_amount
        except Exception as e:
            print(f"  ⚠️ Error inserting trade: {e}")
    
    return inserted, total_pnl, total_fees


async def reconcile_all_users():
    """Reconcile trades for all users with credentials"""
    print("=" * 60)
    print("🔄 TRADE RECONCILIATION (Hyperliquid)")
    print("=" * 60)
    
    pool = await asyncpg.create_pool(DATABASE_URL)
    
    async with pool.acquire() as conn:
        users = await conn.fetch("""
            SELECT id, email, api_key, fee_tier, hl_private_key_encrypted, hl_wallet_address
            FROM follower_users
            WHERE credentials_set = true AND hl_private_key_encrypted IS NOT NULL
        """)
        
        print(f"📋 Found {len(users)} users with credentials")
        
        for user in users:
            print(f"\n{'='*40}")
            print(f"👤 User: {user['email']}")
            
            wallet_address = user.get('hl_wallet_address')
            if not wallet_address:
                pk = decrypt_credential(user['hl_private_key_encrypted'])
                if not pk:
                    print("   ⚠️ Could not decrypt credentials, skipping")
                    continue
                try:
                    wallet_address = Account.from_key(pk).address
                except Exception:
                    print("   ⚠️ Invalid private key, skipping")
                    continue
            
            round_trips = await get_hl_closed_trades(wallet_address, since_days=30)
            if not round_trips:
                print("   📭 No closed trades found")
                continue
            
            inserted, total_pnl, total_fees = await backfill_trades(
                conn, user['id'], round_trips, user['fee_tier'] or 'standard'
            )
            
            status = "🟢" if total_pnl >= 0 else "🔴"
            print(f"   ✅ Inserted {inserted} | {status} P&L: ${total_pnl:.2f} | Fees: ${total_fees:.2f}")
    
    await pool.close()
    print("\n✅ RECONCILIATION COMPLETE")


async def reconcile_single_user(user_id: int):
    """Reconcile trades for a single user"""
    print(f"🔄 Reconciling user {user_id}...")
    pool = await asyncpg.create_pool(DATABASE_URL)
    
    async with pool.acquire() as conn:
        user = await conn.fetchrow("""
            SELECT id, email, api_key, fee_tier, hl_private_key_encrypted, hl_wallet_address
            FROM follower_users WHERE id = $1
        """, user_id)
        
        if not user:
            print(f"❌ User {user_id} not found")
            await pool.close()
            return
        
        wallet_address = user.get('hl_wallet_address')
        if not wallet_address:
            pk = decrypt_credential(user['hl_private_key_encrypted'])
            if not pk:
                print("❌ Could not decrypt credentials")
                await pool.close()
                return
            wallet_address = Account.from_key(pk).address
        
        round_trips = await get_hl_closed_trades(wallet_address, since_days=30)
        if not round_trips:
            print("📭 No closed trades found")
            await pool.close()
            return
        
        inserted, total_pnl, total_fees = await backfill_trades(
            conn, user_id, round_trips, user['fee_tier'] or 'standard'
        )
        print(f"✅ Inserted {inserted} | P&L: ${total_pnl:.2f} | Fees: ${total_fees:.2f}")
    
    await pool.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        asyncio.run(reconcile_single_user(int(sys.argv[1])))
    else:
        asyncio.run(reconcile_all_users())
