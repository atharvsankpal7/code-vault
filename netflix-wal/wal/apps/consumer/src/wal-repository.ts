import { db, wal } from "./db";

type WalRecord = {
  producer: string;
  consumer: string;
  data: Record<string, unknown>;
  retryLeft?: number;
};

export async function saveWalRecord(record: WalRecord) {
  const [savedRecord] = await db.insert(wal).values(record).returning();

  return savedRecord;
}
