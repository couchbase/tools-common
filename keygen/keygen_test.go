package keygen

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	fDel = '%'
	gDel = '#'
)

var testValue = []byte(fmt.Sprintf("{\"stringvalue\": \"value\","+
	"\"emptystring\": \"\","+
	"\"intvalue\": 10,"+
	"\"boolvalue\": true,"+
	"\"floatvalue\": 3.1415,"+
	"\"nested1\": {\"nested2\": {\"nested3\": \"nestedvalue\"}},"+
	"\"nested\": {\"nested\": \"nestedvalue\"},"+
	"\"nestedempty\": {\"nested\": {}},"+
	"\"field.with.dot.\": 1,"+
	"\"backtick`\": 2,"+
	"\"100%%\": 100,"+
	"\"#hello\": \"world\","+
	"\"`nestedbacktick\": {\"a`b\": 3},"+
	"\".nestedHidden\": {\"nested2\": {\"nested3\": 4}},"+
	"\"array\": [\"one\", \"two\", \"three\"],"+
	"\"`.key\": \"backtick1\","+
	"\"`.key.`\": \"backtick2\","+
	"\"nullvalue\": null,"+
	"\"too_long\": \"%s\"}", strings.Repeat(" ", maxKeySize+1)))

func newKeyGenerator(t *testing.T, expr string) KeyGenerator {
	gen, err := NewKeyGenerator(expr, fDel, gDel)
	require.NoError(t, err)

	return gen
}

func assertErrorMsg(t *testing.T, err error, msg string) {
	var expressionError ExpressionError

	require.ErrorAs(t, err, &expressionError)
	require.Equal(t, msg, err.Error())
}

func TestTextOnly(t *testing.T) {
	gen := newKeyGenerator(t, "textonly")

	for i := 0; i < 10; i++ {
		key, err := gen.Next(testValue)
		require.NoError(t, err)
		require.Equal(t, []byte("textonly"), key)
	}
}

func TestNewKeyGeneratorEmptyExpression(t *testing.T) {
	_, err := NewKeyGenerator("", fDel, gDel)
	require.ErrorIs(t, err, ErrEmptyExpression)
}

func TestMonoIncrGenerator(t *testing.T) {
	type test struct {
		expr   string
		offset int
	}

	tests := []*test{
		{
			expr:   "#MONO_INCR#",
			offset: 1,
		},
		{
			expr:   "#MONO_INCR[128]#",
			offset: 128,
		},
		{
			expr:   "#MONO_INCR[0]#",
			offset: 1,
		},
		{
			expr:   "#MONO_INCR[1]#",
			offset: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			gen := newKeyGenerator(t, test.expr)

			for i := 0; i < 10; i++ {
				key, err := gen.Next(testValue)
				require.NoError(t, err)
				require.Equal(t, []byte(strconv.Itoa(i+test.offset)), key)
			}
		})
	}
}

func TestUUIDGenerator(t *testing.T) {
	var (
		expr = "#UUID#"
		gen  = newKeyGenerator(t, expr)
		keys = make([][]byte, 0, 10)
	)

	for i := 0; i < 10; i++ {
		key, err := gen.Next(testValue)
		require.NoError(t, err)
		require.Len(t, key, 36)
		require.NotContains(t, keys, key)
		keys = append(keys, key)
	}
}

func TestGeneratorUnclosedException(t *testing.T) {
	_, err := NewKeyGenerator("#MONO_INCR", fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 10, unclosed generator at end of expression")

	_, err = NewKeyGenerator("%field", fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 6, unclosed field at end of expression")
}

func TestGeneratorFieldEnclosedException(t *testing.T) {
	expr := "#MONO_INCR%field%#"
	_, err := NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 10, attempting to start a field inside a generator")

	expr = "#MONO_INCR%"
	_, err = NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 10, attempting to start a field inside a generator")
}

func TestGeneratorInvalidException(t *testing.T) {
	expr := "#INVALID#"
	_, err := NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 1, invalid generator")
}

func TestFieldAtExpressionEndException(t *testing.T) {
	expr := "text#MONO_INCR#text%"
	_, err := NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 20, start of field at end of expression")

	expr = "text#MONO_INCR#%"
	_, err = NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 16, start of field at end of expression")

	expr = "text%MONO_INCR%%"
	_, err = NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 16, unclosed field at end of expression")
}

func TestGeneratorAtExpressionEndException(t *testing.T) {
	expr := "text#MONO_INCR#text#"
	_, err := NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 20, start of generator at end of expression")

	expr = "text%field1%#"
	_, err = NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 13, start of generator at end of expression")

	expr = "text#MONO_INCR##"
	_, err = NewKeyGenerator(expr, fDel, gDel)
	require.Error(t, err)
	assertErrorMsg(t, err, "error in key expression at char 16, start of generator at end of expression")
}

func TestGeneratorWithText(t *testing.T) {
	gen := newKeyGenerator(t, "before#MONO_INCR#")

	next, err := gen.Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("before1"), next)

	next, err = gen.Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("before2"), next)

	gen = newKeyGenerator(t, "#MONO_INCR#after")

	next, err = gen.Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("1after"), next)

	next, err = gen.Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("2after"), next)

	gen = newKeyGenerator(t, "before#MONO_INCR#after")

	next, err = gen.Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("before1after"), next)

	next, err = gen.Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("before2after"), next)
}

func TestFieldWithStringValue(t *testing.T) {
	gen := newKeyGenerator(t, "%stringvalue%::#MONO_INCR#")

	for i := 0; i < 10; i++ {
		key, err := gen.Next(testValue)
		require.NoError(t, err)
		require.Equal(t, []byte("value::"+strconv.Itoa(i+1)), key)
	}
}

func TestFieldWithBacktickAtStart(t *testing.T) {
	key, err := newKeyGenerator(t, "%```.key`%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("backtick1"), key)
}

func TestFieldWithTrailingBacktick(t *testing.T) {
	_, err := NewKeyGenerator("%field`te%", fDel, gDel)
	require.Error(t, err)
}

func TestFieldWithEscapedBacktick(t *testing.T) {
	key, err := newKeyGenerator(t, "%```.key.```%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("backtick2"), key)
}

func TestFieldNullValue(t *testing.T) {
	var resultError *ResultError

	_, err := newKeyGenerator(t, "%nullvalue%").Next(testValue)
	require.Error(t, err)
	require.ErrorAs(t, err, &resultError)
}

func TestFieldWithStringValueWithEmptyString(t *testing.T) {
	var resultError *ResultError

	_, err := newKeyGenerator(t, "%emptystring%").Next(testValue)
	require.Error(t, err)
	require.ErrorAs(t, err, &resultError)
}

func TestFieldWithStringValueWithKeyToLong(t *testing.T) {
	var resultError *ResultError

	_, err := newKeyGenerator(t, "%too_long%").Next(testValue)
	require.Error(t, err)
	require.ErrorAs(t, err, &resultError)
}

func TestFieldDoesNotExist(t *testing.T) {
	var (
		gen         = newKeyGenerator(t, "%non-existent%::#MONO_INCR#")
		resultError *ResultError
	)

	for i := 0; i < 10; i++ {
		_, err := gen.Next(testValue)
		require.Error(t, err)
		require.ErrorAs(t, err, &resultError)
	}
}

func TestFieldWithIntegerValue(t *testing.T) {
	gen := newKeyGenerator(t, "%intvalue%::#MONO_INCR#")

	for i := 0; i < 10; i++ {
		key, err := gen.Next(testValue)
		require.NoError(t, err)
		require.Equal(t, []byte("10::"+strconv.Itoa(i+1)), key)
	}
}

func TestFieldWithBoolValue(t *testing.T) {
	gen := newKeyGenerator(t, "%boolvalue%::#MONO_INCR#")

	for i := 0; i < 10; i++ {
		key, err := gen.Next(testValue)
		require.NoError(t, err)
		require.Equal(t, []byte("true::"+strconv.Itoa(i+1)), key)
	}
}

func TestFieldWithFloatValue(t *testing.T) {
	gen := newKeyGenerator(t, "%floatvalue%::#MONO_INCR#")

	for i := 0; i < 10; i++ {
		key, err := gen.Next(testValue)
		require.NoError(t, err)
		require.Equal(t, []byte("3.1415::"+strconv.Itoa(i+1)), key)
	}
}

func TestNestedField(t *testing.T) {
	key, err := newKeyGenerator(t, "%nested.nested%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("nestedvalue"), key)
}

func TestDoubleNestedField(t *testing.T) {
	key, err := newKeyGenerator(t, "%nested1.nested2.nested3%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("nestedvalue"), key)
}

func TestFieldWithDot(t *testing.T) {
	key, err := newKeyGenerator(t, "%`field.with.dot.`%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("1"), key)
}

func TestFieldWithBackTick(t *testing.T) {
	key, err := newKeyGenerator(t, "%backtick``%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("2"), key)
}

func TestNestedFieldWithBackTick(t *testing.T) {
	key, err := newKeyGenerator(t, "%``nestedbacktick.a``b%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("3"), key)
}

func TestNestedFieldWithDot(t *testing.T) {
	key, err := newKeyGenerator(t, "%`.nestedHidden`.nested2.nested3%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("4"), key)
}

func TestFieldWithUnbalancedBackTick(t *testing.T) {
	var fieldPathError *FieldPathError

	_, err := NewKeyGenerator("%backtick`%", fDel, gDel)
	require.Error(t, err)
	require.ErrorAs(t, err, &fieldPathError)

	_, err = NewKeyGenerator("%back`tick%", fDel, gDel)
	require.Error(t, err)
	require.ErrorAs(t, err, &fieldPathError)
}

func TestFieldNestedFieldNoName(t *testing.T) {
	var fieldPathError *FieldPathError

	_, err := NewKeyGenerator("%.field%", fDel, gDel)
	require.Error(t, err)
	require.ErrorAs(t, err, &fieldPathError)
}

func TestNestedEmpty(t *testing.T) {
	var resultError *ResultError

	_, err := newKeyGenerator(t, "%nestedempty.nested%").Next(testValue)
	require.Error(t, err)
	require.ErrorAs(t, err, &resultError)
}

func TestNoNameNestedField(t *testing.T) {
	var fieldPathError *FieldPathError

	_, err := NewKeyGenerator("%a..b%", fDel, gDel)
	require.Error(t, err)
	require.ErrorAs(t, err, &fieldPathError)
}

func TestNonExistentNestedField(t *testing.T) {
	var resultError *ResultError

	_, err := newKeyGenerator(t, "%nested.nothere%").Next(testValue)
	require.Error(t, err)
	require.ErrorAs(t, err, &resultError)
}

func TestEscapePoundSignWithGenerator(t *testing.T) {
	key, err := newKeyGenerator(t, "##pound###MONO_INCR###sign##").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("#pound#1#sign#"), key)
}

func TestEscapePoundSign(t *testing.T) {
	key, err := newKeyGenerator(t, "##pound##%stringvalue%##").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("#pound#value#"), key)
}

func TestEscapePercentageSign(t *testing.T) {
	key, err := newKeyGenerator(t, "%%percentage%%sign%%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("%percentage%sign%"), key)
}

func TestEscapePercentageSignWithFieldGenerator(t *testing.T) {
	key, err := newKeyGenerator(t, "%%percentagesign%%%100%%%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("%percentagesign%100"), key)
}

func TestPoundSignInField(t *testing.T) {
	key, err := newKeyGenerator(t, "%#hello%").Next(testValue)
	require.NoError(t, err)
	require.Equal(t, []byte("world"), key)
}

func TestCustomFieldExpresion(t *testing.T) {
	testExp := []string{
		";MONO_INCR;",
		"?stringvalue?",
		"?stringvalue?::;MONO_INCR;",
		"a??b;;",
		"#MONO_INCR#",
	}

	results := []string{
		"1",
		"value",
		"value::1",
		"a?b;",
		"#MONO_INCR#",
	}

	for i, expr := range testExp {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			gen, err := NewKeyGenerator(expr, '?', ';')
			require.NoError(t, err)

			key, err := gen.Next(testValue)
			require.NoError(t, err)
			require.Equal(t, []byte(results[i]), key)
		})
	}
}

func TestValidateDelimiters(t *testing.T) {
	type test struct {
		name string
		fDel rune
		gDel rune
	}

	tests := []*test{
		{
			name: "FieldDelimeterEmpty",
			gDel: gDel,
		},
		{
			name: "GeneratorDelimeterEmpty",
			fDel: fDel,
		},
		{
			name: "FieldDelimeterPeriod",
			fDel: '.',
			gDel: gDel,
		},
		{
			name: "GeneratorDelimeterPeriod",
			fDel: fDel,
			gDel: '.',
		},
		{
			name: "FieldDelimeterBacktick",
			fDel: '`',
			gDel: gDel,
		},
		{
			name: "GeneratorDelimeterBacktick",
			fDel: fDel,
			gDel: '`',
		},
		{
			name: "EqualShouldFail",
			fDel: '-',
			gDel: '-',
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewKeyGenerator("", test.fDel, test.gDel)
			require.Error(t, err)
			require.Error(t, validateDelimiters(test.fDel, test.gDel))
		})
	}
}

func TestFieldPathRemoveFrom(t *testing.T) {
	type test struct {
		name            string
		path            string
		input, expected map[string]interface{}
	}

	tests := []*test{
		{
			name:     "SimpleField",
			path:     "field",
			input:    map[string]interface{}{"field": "value"},
			expected: make(map[string]interface{}),
		},
		{
			name:     "NestedField",
			path:     "nested.field",
			input:    map[string]interface{}{"nested": map[string]interface{}{"field": "value"}},
			expected: map[string]interface{}{"nested": make(map[string]interface{})},
		},
		{
			name:     "NestedFieldNil",
			path:     "nested.field",
			input:    map[string]interface{}{"nested": nil},
			expected: map[string]interface{}{"nested": nil},
		},
		{
			name:     "NestedFieldNotAMap",
			path:     "nested.field",
			input:    map[string]interface{}{"field": "value"},
			expected: map[string]interface{}{"field": "value"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, err := NewFieldPath(test.path)
			require.NoError(t, err)

			path.RemoveFrom(test.input)
			require.Equal(t, test.expected, test.input)
		})
	}
}
