package inventory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// The printer takes an Inventory and writes it. It reads nothing and holds no clock: every age is already
// in the rows.

// WriteJSON writes the inventory as indented JSON (docs/agent-cli.md, "Inventory (JSON)"). HTML escaping is
// off: a purpose holds "<" and ">" and means them.
func WriteJSON(w io.Writer, inv *Inventory) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(inv); err != nil {
		return fmt.Errorf("encode the inventory: %w", err)
	}
	_, err := w.Write(b.Bytes())
	return err
}

// WriteText writes the inventory for a person: a header, then each list, with "none" under an empty one.
// An unknown reads "unknown (<reason>)", as in the JSON's {"unknown": ...}.
func WriteText(w io.Writer, inv *Inventory) error {
	var b strings.Builder
	p := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }
	scope := "namespace " + inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		scope = "every namespace"
	}
	p("inventory at %s, %s", inv.ObservedAt, scope)
	if inv.SelectedOwner != "" {
		p("selected by owner %s", inv.SelectedOwner)
	}

	p("ModelDeployments:")
	for _, d := range inv.ModelDeployments {
		p("  %s/%s: owner %s, purpose %s", d.Namespace, d.Name, d.Owner, quoted(d.Purpose))
		ends := "no end time"
		if d.EndTime != "" {
			ends = "ends " + d.EndTime
		}
		uses := "no Provider"
		if len(d.Providers) > 0 {
			uses = "Providers " + strings.Join(d.Providers, ", ")
		}
		p("    instances %d (ready %s), %s, %s", d.Instances, d.ReadyInstances, ends, uses)
	}
	none(&b, len(inv.ModelDeployments))

	p("Allocations:")
	for _, a := range inv.Allocations {
		p("  %s/%s: ModelDeployment %s, owner %s, purpose %s", a.Namespace, a.Name, a.ModelDeployment, a.Owner, quoted(a.Purpose))
		p("    phase %s, Provider %s, hard end %s", a.Phase, a.Provider, a.HardEnd)
		p("    retention %s", retentionText(a.Retention))
		if !a.IdleSince.IsZero() {
			p("    idle since %s", a.IdleSince)
		}
		if a.KeptNoInstance {
			p("    kept, no Instance")
		}
	}
	none(&b, len(inv.Allocations))

	p("Instances:")
	for _, in := range inv.Instances {
		p("  %s/%s: owner %s, purpose %s, phase %s, Allocation %s", in.Namespace, in.Name, in.Owner, quoted(in.Purpose), in.Phase, in.Allocation)
	}
	none(&b, len(inv.Instances))

	p("Providers:")
	for _, pr := range inv.Providers {
		if pr.LastSuccessfulStatusRead.IsUnknown() {
			p("  %s: last successful read %s", pr.Name, pr.LastSuccessfulStatusRead)
			continue
		}
		p("  %s: last successful read %s, age %s", pr.Name, pr.LastSuccessfulStatusRead, pr.Age)
	}
	none(&b, len(inv.Providers))

	p("Operations awaiting approval:")
	for _, o := range inv.Operations {
		p("  %s/%s: %s of ModelDeployment %s, plan %s, owner %s", o.Namespace, o.Name, o.Action, o.ModelDeployment, o.PlanID, o.Owner)
	}
	none(&b, len(inv.Operations))

	_, err := io.WriteString(w, b.String())
	return err
}

// none writes the line under an empty list.
func none(b *strings.Builder, n int) {
	if n == 0 {
		b.WriteString("  none\n")
	}
}

// quoted is a known text in quotes, since a purpose is free text, and an unknown as it reads.
func quoted(f Fact) string {
	if f.IsUnknown() {
		return f.String()
	}
	return strconv.Quote(f.String())
}

// retentionText is the retention in a phrase: the mode, then each fact that applies.
func retentionText(r RetentionRow) string {
	parts := []string{r.Mode}
	if !r.Until.IsZero() {
		parts = append(parts, "until "+r.Until.String())
	}
	if !r.IdleLimit.IsZero() {
		parts = append(parts, "idle limit "+r.IdleLimit.String())
	}
	if !r.ProfileMaxIdle.IsZero() {
		parts = append(parts, "profile maxIdle "+r.ProfileMaxIdle.String())
	}
	return strings.Join(parts, ", ")
}
