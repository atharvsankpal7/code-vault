CREATE TABLE "wal" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "wal_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"producer" varchar(255) NOT NULL,
	"consumer" varchar(255) NOT NULL,
	"data" jsonb NOT NULL,
	"retryLeft" integer DEFAULT 3 NOT NULL
);
