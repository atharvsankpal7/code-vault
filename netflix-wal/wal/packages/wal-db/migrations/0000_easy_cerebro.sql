CREATE TYPE "public"."outbox_status" AS ENUM('pending', 'processing', 'done', 'failed');--> statement-breakpoint
CREATE TABLE "wal_outbox" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"status" "outbox_status" NOT NULL,
	"topic_name" text NOT NULL,
	"message" text NOT NULL,
	"error" jsonb,
	"worker_id" text,
	"process_started_at" timestamp,
	"processing_time_out" timestamp,
	"process_completed_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
