package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/net/context/ctxhttp"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/grafana/grafana/pkg/components/null"
	"github.com/grafana/grafana/pkg/log"
	"github.com/grafana/grafana/pkg/models"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/tsdb"
)

type ClickhouseQueryEndpoint struct {
	log        log.Logger
	dataSource *models.DataSource
	httpClient *http.Client
}

func NewClickhouseQueryEndpoint(dataSource *models.DataSource) (tsdb.TsdbQueryEndpoint, error) {
	httpClient, err := dataSource.GetHttpClient()

	if err != nil {
		return nil, err
	}

	return &ClickhouseQueryEndpoint{
		log:        log.New("tsdb.clickhouse"),
		dataSource: dataSource,
		httpClient: httpClient,
	}, nil
}

func init() {
	tsdb.RegisterTsdbQueryEndpoint("vertamedia-clickhouse-datasource", NewClickhouseQueryEndpoint)
}

func (e *ClickhouseQueryEndpoint) Query(ctx context.Context, dsInfo *models.DataSource, tsdbQuery *tsdb.TsdbQuery) (*tsdb.Response, error) {
	result := &tsdb.Response{}
	params := url.Values{}
	params["query"] = getQuery(tsdbQuery)

	if setting.Env == setting.DEV {
		e.log.Debug("Clickhouse request", "params", params)
	}

	req, err := e.createRequest(params, dsInfo)
	if err != nil {
		return nil, err
	}
	res, err := ctxhttp.Do(ctx, e.httpClient, req)
	if err != nil {
		return nil, err
	}

	dto, err := e.parseResponse(res)
	if err != nil {
		return nil, err
	}

	queryRes, err := convertToResult(dto)
	if err != nil {
		e.log.Info("Failed to convert response", "error", err)
		return nil, err
	}

	result.Results = make(map[string]*tsdb.QueryResult)
	result.Results["A"] = queryRes
	return result, err
}

func getQuery(tsdbQuery *tsdb.TsdbQuery) []string {
	query := ""
	timeFilterQuery := tsdbQuery.Queries[0].Model.Get("timeFilterQuery").MustString()
	if timeFilterQuery == "" {
		query = tsdbQuery.Queries[0].Model.Get("rawQuery").MustString()
	} else {
		fromSecs := tsdbQuery.TimeRange.GetFromAsSecondsEpoch()
		toSecs := tsdbQuery.TimeRange.GetToAsSecondsEpoch()
		r := strings.NewReplacer(
			"$from", strconv.FormatInt(fromSecs, 10),
			"$to", strconv.FormatInt(toSecs, 10),
		)
		query = r.Replace(timeFilterQuery)
	}
	return []string{query + " FORMAT JSON"}
}

func (e *ClickhouseQueryEndpoint) createRequest(data url.Values, dsInfo *models.DataSource) (*http.Request, error) {
	u, _ := url.Parse(dsInfo.Url)
	u.RawQuery = data.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		e.log.Info("Failed to create request", "error", err)
		return nil, fmt.Errorf("Failed to create request. error: %v", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if dsInfo.BasicAuth {
		req.SetBasicAuth(dsInfo.BasicAuthUser, dsInfo.BasicAuthPassword)
	}

	return req, err
}

func (e *ClickhouseQueryEndpoint) parseResponse(res *http.Response) (*TargetResponseDTO, error) {
	var data TargetResponseDTO

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()

	if err != nil {
		return nil, err
	}

	if res.StatusCode/100 != 2 {
		e.log.Info("Request failed", "status", res.Status, "body", string(body))
		return nil, fmt.Errorf("Request failed status: %v", res.Status)
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		e.log.Info("Failed to unmarshal clickhouse response", "error", err, "status", res.Status, "body", string(body))
		return nil, err
	}

	return &data, nil
}

func convertToResult(dto *TargetResponseDTO) (*tsdb.QueryResult, error) {
	if len(dto.MetaData) < 2 {
		return nil, fmt.Errorf("Response meta-data contains %d values. Expect at least two values", len(dto.MetaData))
	}

	var timeColumnName string
	// name of the time column always should be first
	if dto.MetaData[0].Type != "UInt64" {
		return nil, fmt.Errorf("First element in metadata must be UInt64")
	}

	queryRes := tsdb.NewQueryResult()
	timeColumnName = dto.MetaData[0].Name
	timeSeries := make(map[string]map[float64]float64)
	timeStamps := make([]float64, 0, len(dto.Data))

	for _, series := range dto.Data {
		timeValue, err := extractFloat64(series[timeColumnName])
		if err != nil {
			return nil, fmt.Errorf("Error while parsing timestamp value: %s", err)
		}

		timeStamps = append(timeStamps, timeValue)
		delete(series, timeColumnName)

		for k, v := range series {
			pointValue, err := extractFloat64(v)
			if err == nil {
				if _, ok := timeSeries[k]; !ok {
					timeSeries[k] = make(map[float64]float64)
				}
				timeSeries[k][timeValue] = pointValue
				continue
			}

			// Since ClickHouse may return arrays if using `groupArray` we need to process it right
			// No guarantees that `groupArray` will return same number of names in each array
			// That's why we need to create them by ourselves to provide same amount of datapoints
			if reflect.TypeOf(v).Kind() != reflect.Slice {
				return nil, fmt.Errorf("PointValue parsing error: %s", err)
			}

			groupArr := v.([]interface{})
			for _, group := range groupArr {
				tuple, ok := group.([]interface{})
				if !ok {
					return nil, fmt.Errorf("Unexpected tuple type: %T. Expected []interface{}", tuple)
				}
				if len(tuple) != 2 {
					return nil, fmt.Errorf("Unexpected tuple array length: %d. Expected 2", len(tuple))
				}

				key, ok := tuple[0].(string)
				if !ok {
					return nil, fmt.Errorf("Unexpected key type: %T. Expected string", key)
				}

				pointValue, err := extractFloat64(tuple[1])
				if err != nil {
					return nil, fmt.Errorf("PointValue parsing error: %s", err)
				}

				if _, ok := timeSeries[key]; !ok {
					timeSeries[key] = make(map[float64]float64)
				}
				timeSeries[key][timeValue] = pointValue
			}
		}
	}

	// Grafana developers made announce that alerts will be supported by singlestat and tables
	// but now all datasources working only with tsdb.TimeSeries
	for target, points := range timeSeries {
		dataPoints := tsdb.TimeSeriesPoints{}
		for _, t := range timeStamps {
			point := tsdb.NewTimePoint(null.FloatFrom(points[t]), t)
			dataPoints = append(dataPoints, point)
		}

		queryRes.Series = append(queryRes.Series, &tsdb.TimeSeries{
			Name:   target,
			Points: dataPoints,
		})
	}

	return queryRes, nil
}

func extractFloat64(v interface{}) (float64, error) {
	switch v.(type) {
	case string:
		return strconv.ParseFloat(v.(string), 64)
	case float64:
		return v.(float64), nil
	case nil:
		return float64(0), nil
	default:
		return 0, fmt.Errorf("Unexpected type: %T", v)
	}
}
