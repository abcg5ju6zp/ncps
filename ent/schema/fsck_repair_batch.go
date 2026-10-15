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

// FsckRepairBatch is one crash-recoverable `fsck --repair` run. Before any
// mutation, the reviewed (phase-2 re-verified) issue list is frozen into
// FsckRepairItem rows carrying the object identity, the facts discovered at
// scan time, and the expected action. A process killed mid-repair leaves the
// batch non-terminal; the next repair invocation adopts it and continues its
// unfinished items instead of rescanning every object.
//
// At most one batch is active at a time. The partial-index approach is not
// portable to MySQL, so active_slot is set to a fixed value for non-terminal
// batches and cleared (NULL) on terminal ones: a UNIQUE index permits many
// NULLs on every supported dialect, so concurrent batch creation loses on the
// constraint and only one batch can ever be adopted.
//
// Single-executor safety: each invocation that adopts a batch records its
// executor_id (random UUID), bumps fence_token, and holds a renewable lease
// (lease_expires_at). Every state transition is a conditional UPDATE that
// must match (executor_id, fence_token, unexpired lease); an executor whose
// lease expired and was taken over keeps its old fence token and can no
// longer commit late results.
//
// status values (plain string, not an enum, to stay dialect-portable):
//   - "running":                 created, executor is or should be advancing it.
//   - "cancelled":               the executor stopped on cancel/timeout/exit
//     with unfinished items; eligible for adoption.
//   - "completed":               every item reached done/skipped.
//   - "completed_with_errors":   drained, but some items failed or await retry;
//     terminal — a later fresh scan replans them.
type FsckRepairBatch struct {
	ent.Schema
}

// Annotations pins the on-disk table name.
func (FsckRepairBatch) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "fsck_repair_batches"},
		entsql.Annotation{
			//nolint:gosec // the CHECK name contains "token"; it is a monotonic counter, not a credential
			Checks: map[string]string{
				"fsck_repair_batches_fence_token_nonneg": "fence_token >= 0",
			},
		},
	}
}

// Mixin contributes created_at / updated_at.
func (FsckRepairBatch) Mixin() []ent.Mixin {
	return []ent.Mixin{entmixin.Timestamps{}}
}

// Fields of the FsckRepairBatch.
func (FsckRepairBatch) Fields() []ent.Field {
	return []ent.Field{
		// uid is the stable, operator-facing batch identifier (UUID text).
		field.String("uid").NotEmpty(),
		// status is one of the status constants in pkg/ncps/fsck_repair.go.
		field.String("status").Default("running"),
		// active_slot enforces the single-active-batch invariant; see type doc.
		field.Int("active_slot").
			Optional().
			Nillable(),
		// consent records how mutation was authorized: the --repair flag
		// ("repair") or an interactive confirmation ("interactive").
		field.String("consent").Default("repair"),
		// cdc_mode / verify_content freeze the scan configuration so an
		// adopted batch knows whether the chunk store is required.
		field.Bool("cdc_mode").Default(false),
		field.Bool("verify_content").Default(false),
		// executor identity and lease/fencing state.
		field.String("executor_id").Default(""),
		field.String("executor_name").Default(""),
		field.Int("fence_token").Default(0),
		field.Time("lease_expires_at").
			Optional().
			Nillable(),
		field.Time("started_at").
			Optional().
			Nillable(),
		field.Time("finished_at").
			Optional().
			Nillable(),
		// summary holds the final JSON report (counts by status/issue type).
		field.Text("summary").
			Optional().
			Nillable(),
	}
}

// Edges of the FsckRepairBatch.
func (FsckRepairBatch) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("items", FsckRepairItem.Type).
			Annotations(entsql.OnDelete(entsql.Cascade)),
	}
}

// Indexes of the FsckRepairBatch.
func (FsckRepairBatch) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("uid").Unique(),
		index.Fields("active_slot").Unique(),
		index.Fields("status"),
	}
}
