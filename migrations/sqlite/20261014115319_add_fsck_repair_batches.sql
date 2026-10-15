-- +goose Up
-- create "fsck_repair_batches" table
CREATE TABLE `fsck_repair_batches` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `created_at` datetime NOT NULL DEFAULT (CURRENT_TIMESTAMP), `updated_at` datetime NULL, `uid` text NOT NULL, `status` text NOT NULL DEFAULT ('running'), `active_slot` integer NULL, `consent` text NOT NULL DEFAULT ('repair'), `cdc_mode` bool NOT NULL DEFAULT (false), `verify_content` bool NOT NULL DEFAULT (false), `executor_id` text NOT NULL DEFAULT (''), `executor_name` text NOT NULL DEFAULT (''), `fence_token` integer NOT NULL DEFAULT (0), `lease_expires_at` datetime NULL, `started_at` datetime NULL, `finished_at` datetime NULL, `summary` text NULL, CONSTRAINT `fsck_repair_batches_fence_token_nonneg` CHECK (fence_token >= 0));
-- create index "fsckrepairbatch_uid" to table: "fsck_repair_batches"
CREATE UNIQUE INDEX `fsckrepairbatch_uid` ON `fsck_repair_batches` (`uid`);
-- create index "fsckrepairbatch_active_slot" to table: "fsck_repair_batches"
CREATE UNIQUE INDEX `fsckrepairbatch_active_slot` ON `fsck_repair_batches` (`active_slot`);
-- create index "fsckrepairbatch_status" to table: "fsck_repair_batches"
CREATE INDEX `fsckrepairbatch_status` ON `fsck_repair_batches` (`status`);
-- create "fsck_repair_items" table
CREATE TABLE `fsck_repair_items` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `created_at` datetime NOT NULL DEFAULT (CURRENT_TIMESTAMP), `updated_at` datetime NULL, `uid` text NOT NULL, `seq` integer NOT NULL, `kind` text NOT NULL DEFAULT ('object'), `issue_type` text NOT NULL, `action` text NOT NULL, `object_key` text NOT NULL DEFAULT (''), `object_id` integer NULL, `facts` text NULL, `status` text NOT NULL DEFAULT ('pending'), `phase` text NOT NULL DEFAULT (''), `attempts` integer NOT NULL DEFAULT (0), `last_error` text NULL, `skip_reason` text NULL, `claimed_by` text NOT NULL DEFAULT (''), `claimed_at` datetime NULL, `completed_at` datetime NULL, `batch_id` integer NOT NULL, CONSTRAINT `fsck_repair_items_fsck_repair_batches_items` FOREIGN KEY (`batch_id`) REFERENCES `fsck_repair_batches` (`id`) ON DELETE CASCADE, CONSTRAINT `fsck_repair_items_attempts_nonneg` CHECK (attempts >= 0), CONSTRAINT `fsck_repair_items_seq_nonneg` CHECK (seq >= 0));
-- create index "fsckrepairitem_batch_id_uid" to table: "fsck_repair_items"
CREATE UNIQUE INDEX `fsckrepairitem_batch_id_uid` ON `fsck_repair_items` (`batch_id`, `uid`);
-- create index "fsckrepairitem_batch_id_seq" to table: "fsck_repair_items"
CREATE INDEX `fsckrepairitem_batch_id_seq` ON `fsck_repair_items` (`batch_id`, `seq`);
-- create index "fsckrepairitem_batch_id_status" to table: "fsck_repair_items"
CREATE INDEX `fsckrepairitem_batch_id_status` ON `fsck_repair_items` (`batch_id`, `status`);

-- +goose Down
-- reverse: create index "fsckrepairitem_batch_id_status" to table: "fsck_repair_items"
DROP INDEX `fsckrepairitem_batch_id_status`;
-- reverse: create index "fsckrepairitem_batch_id_seq" to table: "fsck_repair_items"
DROP INDEX `fsckrepairitem_batch_id_seq`;
-- reverse: create index "fsckrepairitem_batch_id_uid" to table: "fsck_repair_items"
DROP INDEX `fsckrepairitem_batch_id_uid`;
-- reverse: create "fsck_repair_items" table
DROP TABLE `fsck_repair_items`;
-- reverse: create index "fsckrepairbatch_status" to table: "fsck_repair_batches"
DROP INDEX `fsckrepairbatch_status`;
-- reverse: create index "fsckrepairbatch_active_slot" to table: "fsck_repair_batches"
DROP INDEX `fsckrepairbatch_active_slot`;
-- reverse: create index "fsckrepairbatch_uid" to table: "fsck_repair_batches"
DROP INDEX `fsckrepairbatch_uid`;
-- reverse: create "fsck_repair_batches" table
DROP TABLE `fsck_repair_batches`;
