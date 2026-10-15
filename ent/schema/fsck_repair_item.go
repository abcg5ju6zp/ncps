package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"

	"github.com/kalbasit/ncps/internal/entmixin"
)

// FsckRepairItem is one frozen, crash-recoverable unit of work inside an
// FsckRepairBatch. Each item records the object identity (natural key, never
// just a row id), the facts captured at scan/re-verify time (JSON), the
// expected action, and the precise phase the action has advanced to. A
// terminal status is written only after the database AND storage end states
// have been confirmed; partial completion is resumed from `phase` on the next
// run (every action is idempotent and end-state-verified).
//
// status values (plain string, not an enum, to stay dialect-portable):
//   - "pending":       frozen, not yet claimed.
//   - "in_progress":   claimed by an executor; phase records the checkpoint.
//   - "done":          expected end state confirmed in database and storage.
//   - "skipped":       re-validation showed the object healed or changed
//                      (skip_reason says why).
//   - "retry_pending": a transient/undeterminable error blocked completion;
//                      retained for the next run.
//   - "failed":        a deterministic failure (re-validated on adoption).
//
// kind is "object" for per-object repairs and "phase" for batch-level
// checkpointed phases (stale-link reclamation, narinfo compression repair).
type FsckRepairItem struct {
	ent.Schema
}

// Annotations pins the on-disk table name and table-level CHECK constraints
// (field-level checks are dropped by Ent codegen — see invariant A1).
func (FsckRepairItem) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "fsck_repair_items"},
		entsql.Annotation{
			Checks: map[string]string{
				"fsck_repair_items_seq_nonneg":      "seq >= 0",
				"fsck_repair_items_attempts_nonneg": "attempts >= 0",
			},
		},
	}
}

// Mixin contributes created_at / updated_at.
func (FsckRepairItem) Mixin() []ent.Mixin {
	return []ent.Mixin{entmixin.Timestamps{}}
}

// Fields of the FsckRepairItem.
func (FsckRepairItem) Fields() []ent.Field {
	return []ent.Field{
		field.Int("batch_id"),
		// uid is the deterministic item identifier, unique within a batch, so
		// derived items discovered during repair are inserted idempotently.
		field.String("uid").NotEmpty(),
		// seq fixes claim order, mirroring the historical repair category order.
		field.Int("seq"),
		field.String("kind").Default("object"),
		// issue_type is the stable classification code shared with the fsck
		// report; action is the machine-executed repair verb.
		field.String("issue_type"),
		field.String("action"),
		// object identity: natural key plus the row id observed at freeze time.
		field.String("object_key").Default(""),
		field.Int("object_id").
			Optional().
			Nillable(),
		// facts is the JSON snapshot of the evidence gathered at scan time.
		field.Text("facts").
			Optional().
			Nillable(),
		field.String("status").Default("pending"),
		// phase is the latest durable checkpoint inside action ("" = not started).
		field.String("phase").Default(""),
		field.Int("attempts").Default(0),
		field.Text("last_error").
			Optional().
			Nillable(),
		field.Text("skip_reason").
			Optional().
			Nillable(),
		field.String("claimed_by").Default(""),
		field.Time("claimed_at").
			Optional().
			Nillable(),
		field.Time("completed_at").
			Optional().
			Nillable(),
	}
}

// Edges of the FsckRepairItem.
func (FsckRepairItem) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("batch", FsckRepairBatch.Type).
			Ref("items").
			Field("batch_id").
			Unique().
			Required(),
	}
}

// Indexes of the FsckRepairItem.
func (FsckRepairItem) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("batch_id", "uid").Unique(),
		index.Fields("batch_id", "seq"),
		index.Fields("batch_id", "status"),
	}
}
