"""Tests for agtype serialization utilities."""

from age_orm.utils.serialization import (
    escape_agtype_string,
    escape_sql_literal,
    to_agtype_value,
    to_agtype_properties,
    format_cypher_value,
    substitute_cypher_params,
    model_to_cypher_properties,
)


class TestEscapeAgtypeString:
    def test_simple_string(self):
        assert escape_agtype_string("hello") == "hello"

    def test_backslash(self):
        assert escape_agtype_string("back\\slash") == "back\\\\slash"

    def test_double_quote(self):
        assert escape_agtype_string('say "hi"') == 'say \\"hi\\"'

    def test_newline(self):
        assert escape_agtype_string("line1\nline2") == "line1\\nline2"

    def test_tab(self):
        assert escape_agtype_string("col1\tcol2") == "col1\\tcol2"

    def test_carriage_return(self):
        assert escape_agtype_string("line1\rline2") == "line1\\rline2"

    def test_control_chars(self):
        result = escape_agtype_string("\x01\x02")
        assert result == "\\u0001\\u0002"

    def test_none_input(self):
        assert escape_agtype_string(None) == ""

    def test_mixed_special_chars(self):
        result = escape_agtype_string('He said "hello\\world"\n')
        assert result == 'He said \\"hello\\\\world\\"\\n'


class TestEscapeSqlLiteral:
    def test_no_quotes(self):
        assert escape_sql_literal("hello") == "hello"

    def test_single_quote(self):
        assert escape_sql_literal("it's") == "it''s"

    def test_multiple_quotes(self):
        assert escape_sql_literal("'a' and 'b'") == "''a'' and ''b''"


class TestToAgtypeValue:
    def test_none(self):
        assert to_agtype_value(None) == "null"

    def test_true(self):
        assert to_agtype_value(True) == "true"

    def test_false(self):
        assert to_agtype_value(False) == "false"

    def test_int(self):
        assert to_agtype_value(42) == "42"

    def test_float(self):
        assert to_agtype_value(3.14) == "3.14"

    def test_string(self):
        assert to_agtype_value("hello") == '"hello"'

    def test_string_with_quotes(self):
        result = to_agtype_value('say "hi"')
        assert result == '"say \\"hi\\""'

    def test_list(self):
        result = to_agtype_value([1, "a", True])
        assert result == '[1, "a", true]'

    def test_dict(self):
        result = to_agtype_value({"key": "val", "num": 1})
        assert result == '{"key": "val", "num": 1}'

    def test_nested(self):
        result = to_agtype_value({"list": [1, 2], "nested": {"a": "b"}})
        assert '"list": [1, 2]' in result
        assert '"nested": {"a": "b"}' in result

    def test_fallback_to_string(self):
        """Non-standard types get stringified."""

        class Custom:
            def __str__(self):
                return "custom_value"

        result = to_agtype_value(Custom())
        assert result == '"custom_value"'


class TestToAgtypeProperties:
    def test_simple(self):
        result = to_agtype_properties({"name": "Alice", "age": 30})
        assert '"name": "Alice"' in result
        assert '"age": 30' in result
        assert result.startswith("{")
        assert result.endswith("}")

    def test_empty(self):
        assert to_agtype_properties({}) == "{}"


class TestFormatCypherValue:
    def test_none(self):
        assert format_cypher_value(None) == "null"

    def test_bool(self):
        assert format_cypher_value(True) == "true"
        assert format_cypher_value(False) == "false"

    def test_int(self):
        assert format_cypher_value(42) == "42"

    def test_float(self):
        assert format_cypher_value(3.14) == "3.14"

    def test_string(self):
        assert format_cypher_value("hello") == "'hello'"

    def test_string_with_single_quote(self):
        assert format_cypher_value("it's") == "'it\\'s'"

    def test_list(self):
        result = format_cypher_value([1, "a"])
        assert result == "[1, 'a']"

    def test_dict(self):
        result = format_cypher_value({"k": "v"})
        assert result == "{k: 'v'}"


class TestSubstituteCypherParams:
    def test_no_params(self):
        cypher = "MATCH (n) RETURN n"
        assert substitute_cypher_params(cypher, None) == cypher

    def test_single_param(self):
        result = substitute_cypher_params(
            "MATCH (n) WHERE n.age > $min_age RETURN n", {"min_age": 20}
        )
        assert "$min_age" not in result
        assert "20" in result

    def test_multiple_params(self):
        result = substitute_cypher_params(
            "WHERE n.name = $name AND n.age > $age",
            {"name": "Alice", "age": 25},
        )
        assert "'Alice'" in result
        assert "25" in result

    def test_no_partial_replacement(self):
        """$age should not be replaced inside $age_max."""
        result = substitute_cypher_params(
            "WHERE n.age > $age AND n.age < $age_max",
            {"age": 20, "age_max": 50},
        )
        assert "20" in result
        assert "50" in result

    def test_string_param(self):
        result = substitute_cypher_params(
            "WHERE n.name = $name", {"name": "Bob"}
        )
        assert "'Bob'" in result


class TestModelToCypherProperties:
    def test_basic(self):
        from tests.conftest import Person

        p = Person(name="Alice", age=30)
        result = model_to_cypher_properties(p)
        assert "name: 'Alice'" in result
        assert "age: 30" in result
        assert result.startswith("{")
        assert result.endswith("}")

    def test_with_only_filter(self):
        from tests.conftest import Person

        p = Person(name="Alice", age=30, email="a@b.com")
        result = model_to_cypher_properties(p, only={"name"})
        assert "name: 'Alice'" in result
        assert "age" not in result
        assert "email" not in result
