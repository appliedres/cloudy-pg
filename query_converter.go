package cloudypg

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/appliedres/cloudy/datastore"
)

type PgQueryConverter struct {
}

func (qc *PgQueryConverter) Convert(c *datastore.SimpleQuery, table string) string {
	sql := qc.ConvertSelect(c, table)

	where := qc.ConvertConditionGroup(c.Conditions)
	if where != "" {
		sql += fmt.Sprintf(" WHERE %s", where)
	}

	sort := qc.ConvertSort(c.SortBy)
	if sort != "" {
		sql += fmt.Sprintf(" ORDER BY %s", sort)
	}

	if c.Size > 0 {
		sql += fmt.Sprintf(" LIMIT %v", c.Size)
	}

	if c.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %v", c.Offset)
	}

	// SELECT columns FROM table where conditions limit offset
	return sql
}

func (qc *PgQueryConverter) ConvertSelect(c *datastore.SimpleQuery, table string) string {
	columns := "data"

	if len(c.Colums) > 0 {
		jsonQuery := []string{columns}
		for _, col := range c.Colums {
			jsonQuery = append(jsonQuery, fmt.Sprintf("%v as \"%v\"", qc.toField(col), col))
		}
		columns = strings.Join(jsonQuery, ", ")
	}

	return fmt.Sprintf("SELECT %s FROM %s", columns, table)
}

func (qc *PgQueryConverter) ConvertSort(sortbys []*datastore.SortBy) string {
	if len(sortbys) == 0 {
		return ""
	}
	var sorts []string
	for _, sortBy := range sortbys {
		sort := qc.ConvertASort(sortBy)
		if sort != "" {
			sorts = append(sorts, sort)
		}
	}
	return strings.Join(sorts, ", ")
}

func (qc *PgQueryConverter) ConvertASort(c *datastore.SortBy) string {
	f := qc.toField(c.Field)
	if c.Descending {
		return f + " DESC"
	} else {
		return f + " ASC"
	}
}
func (qc *PgQueryConverter) toJsonField(path string) string {
	p := gabs.DotPathToSlice(path)
	if len(p) > 1 {
		last := p[len(p)-1] // Get the last element
		leading := p[:len(p)-1]
		path = fmt.Sprintf("->'%v'->'%v'", strings.Join(leading, "'->'"), last)
	} else {
		path = fmt.Sprintf("->'%v'", path)
	}
	return fmt.Sprintf("data%v", path)
}

func (qc *PgQueryConverter) toField(path string) string {
	p := gabs.DotPathToSlice(path)
	if len(p) > 1 {
		last := p[len(p)-1] // Get the last element
		leading := p[:len(p)-1]
		path = fmt.Sprintf("->'%v'->>'%v'", strings.Join(leading, "'->'"), last)
	} else {
		path = fmt.Sprintf("->>'%v'", path)
	}
	return fmt.Sprintf("data%v", path)
}
func (qc *PgQueryConverter) toFieldArr(path string) string {
	p := gabs.DotPathToSlice(path)
	if len(p) > 1 {
		last := p[len(p)-1] // Get the last element
		leading := p[:len(p)-1]
		path = fmt.Sprintf("->'%v'->'%v'", strings.Join(leading, "'->'"), last)
	} else {
		path = fmt.Sprintf("->'%v'", path)
	}
	return fmt.Sprintf("data%v", path)
}

func (qc *PgQueryConverter) ConvertCondition(c *datastore.SimpleQueryCondition) string {
	switch c.Type {
	case "eq":
		return fmt.Sprintf("(%v) = '%v'", qc.toField(c.Data[0]), c.Data[1])
	case "neq":
		return fmt.Sprintf("(%v) != '%v'", qc.toField(c.Data[0]), c.Data[1])
	case "between":
		return fmt.Sprintf("(%v)::numeric BETWEEN %v AND %v", qc.toField(c.Data[0]), c.Data[1], c.Data[2])
	case "lt":
		return fmt.Sprintf("(%v)::numeric < %v", qc.toField(c.Data[0]), c.Data[1])
	case "lte":
		return fmt.Sprintf("(%v)::numeric  <= %v", qc.toField(c.Data[0]), c.Data[1])
	case "gt":
		return fmt.Sprintf("(%v)::numeric  > %v", qc.toField(c.Data[0]), c.Data[1])
	case "gte":
		return fmt.Sprintf("(%v)::numeric  >= %v", qc.toField(c.Data[0]), c.Data[1])
	case "before":
		val := c.GetDate("value")
		if !val.IsZero() {
			timestr := val.UTC().Format(time.RFC3339)
			// return fmt.Sprintf("(data->'%v')::timestamptz < '%v'", c.Data[0], timestr)
			// return fmt.Sprintf("to_date((%v), 'YYYY-MM-DDTHH24:MI:SS.MSZ') < '%v'", c.Data[0], timestr)
			return fmt.Sprintf("(%v)::timestamptz < '%v'", qc.toField(c.Data[0]), timestr)
		}
	case "after":
		val := c.GetDate("value")
		if !val.IsZero() {
			timestr := val.UTC().Format(time.RFC3339)
			// return fmt.Sprintf("(data->'%v')::timestamptz > '%v'", c.Data[0], timestr)
			// return fmt.Sprintf("to_date((%v), 'YYYY-MM-DDTHH24:MI:SS.MSZ') > '%v'", c.Data[0], timestr)
			return fmt.Sprintf("(%v)::timestamptz > '%v'", qc.toField(c.Data[0]), timestr)
		}
	case "?":
		return fmt.Sprintf("(%v)::numeric  ? '%v'", qc.toField(c.Data[0]), c.Data[1])
	case "contains":
		return fmt.Sprintf("(%v)::jsonb @> '[\"%v\"]'", qc.toFieldArr(c.Data[0]), c.Data[1])
	case "includes":
		values := c.GetStringArr("value")
		var xformed []string
		for _, v := range values {
			xformed = append(xformed, fmt.Sprintf("'%v'", v))
		}
		if values != nil {
			return fmt.Sprintf("(%v) in (%v)", qc.toField(c.Data[0]), strings.Join(xformed, ","))
		}
	case "in":
		return fmt.Sprintf("(%v)::jsonb ? '%v'", qc.toJsonField(c.Data[0]), c.Data[1])
		// return "(data::jsonb->'users' ? 'test-user@example.com')"
	case "anyin":
		values := c.GetStringArr("value")
		var xformed []string
		for _, v := range values {
			xformed = append(xformed, fmt.Sprintf("'%v'", v))
		}
		vals := strings.Join(xformed, ",")
		return fmt.Sprintf("(%v)::jsonb  ?| ARRAY[%v]", qc.toJsonField(c.Data[0]), vals)
	case "null":
		return fmt.Sprintf("(%v) IS NULL", qc.toField(c.Data[0]))
	}
	return "UNKNOWN"
}

func (qc *PgQueryConverter) ConvertConditionGroup(cg *datastore.SimpleQueryConditionGroup) string {
	if len(cg.Conditions) == 0 && len(cg.Groups) == 0 {
		return ""
	}

	var conditionStr []string
	for _, c := range cg.Conditions {
		conditionStr = append(conditionStr, qc.ConvertCondition(c))
	}
	for _, c := range cg.Groups {
		result := qc.ConvertConditionGroup(c)
		if result != "" {
			conditionStr = append(conditionStr, "( "+result+" )")
		}
	}
	return strings.Join(conditionStr, " "+cg.Operator+" ")
}

func (qc *PgQueryConverter) ToColumnName(name string) string {
	return qc.toField(name)
}
