package bootstrap

import (
	"errors"
	"reflect"
	"testing"
)

func TestValidateRunListAcceptsChefCompatibleItemShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  []string
	}{
		{
			name:  "non_normalized_run_list",
			value: []any{"foo", "foo::bar", "bar::baz@1.0.0", "recipe[web]", "role[prod]"},
			want:  []string{"foo", "foo::bar", "bar::baz@1.0.0", "recipe[web]", "role[prod]"},
		},
		{
			name: "numeric_and_versioned_items",
			value: []any{
				"1",
				"recipe[1]",
				"role[1]",
				"base@1.0",
				"base@1.0.1",
				"recipe[base@1.0]",
				"recipe[base@1.0.1]",
			},
			want: []string{
				"1",
				"recipe[1]",
				"role[1]",
				"base@1.0",
				"base@1.0.1",
				"recipe[base@1.0]",
				"recipe[base@1.0.1]",
			},
		},
		{
			name: "oddly_named_recipe_and_role_items",
			value: []string{
				"recipe",
				"recipe::foo",
				"recipe::bar@1.0.0",
				"role",
				"role::foo",
				"role::bar@1.0.0",
				"recipe[recipe]",
				"recipe[role]",
				"role[recipe]",
				"role[role]",
			},
			want: []string{
				"recipe",
				"recipe::foo",
				"recipe::bar@1.0.0",
				"role",
				"role::foo",
				"role::bar@1.0.0",
				"recipe[recipe]",
				"recipe[role]",
				"role[recipe]",
				"role[role]",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := validateRunList(tt.value)
			if err != nil {
				t.Fatalf("validateRunList() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("validateRunList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateRunListRejectsChefIncompatibleItemShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "one_part_unqualified_version", value: []any{"gibberish@1"}},
		{name: "one_part_qualified_recipe_version", value: []any{"recipe[gibberish@1]"}},
		{name: "qualified_role_version", value: []any{"role[gibberish@1.0]"}},
		{name: "single_colon_separator", value: []any{"foo:bar"}},
		{name: "triple_colon_separator", value: []any{"foo:::bar"}},
		{name: "qualified_role_with_colon", value: []any{"role[foo:bar]"}},
		{name: "unicode_unqualified_recipe", value: []any{"漢字"}},
		{name: "unicode_qualified_recipe", value: []any{"recipe[漢字]"}},
		{name: "empty_item", value: []any{""}},
		{name: "bogus_bracketed_item", value: []any{"node[gibberish]"}},
		{name: "symbol_heavy_item", value: []any{"[gibberish]#@$%"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := validateRunList(tt.value)
			if err == nil {
				t.Fatal("validateRunList() error = nil, want validation error")
			}

			var validationErr *ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("validateRunList() error = %T, want *ValidationError", err)
			}
			if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "Field 'run_list' is not a valid run list" {
				t.Fatalf("validation messages = %v, want invalid run_list message", validationErr.Messages)
			}
		})
	}
}
