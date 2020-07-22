package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"text/template"
	"time"

	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx/v4"
)

var logger kitlog.Logger

var (
	app = kingpin.New("xid-for-time", "Find the last xid that committed before time").Version("1.0.0")

	table            = app.Arg("table", "Table to use for estimates").Required().String()
	targetTimeString = app.Arg("time", "Target time to compute xid for").Required().String()

	// Database connection paramters
	host     = app.Flag("host", "Postgres host").Envar("PGHOST").Default("127.0.0.1").String()
	port     = app.Flag("port", "Postgres port").Envar("PGPORT").Default("5432").Uint16()
	database = app.Flag("database", "Postgres database name").Envar("PGDATABASE").Default("postgres").String()
	user     = app.Flag("user", "Postgres user").Envar("PGUSER").Default("postgres").String()
)

const (
	selectThresholds = `
select * from (
    select id as min_id
         , created_at as min_created_at
         , lag(id, 1) over(order by created_at desc) as max_id
         , lag(created_at, 1) over(order by created_at desc) as max_created_at
      from (
          select id
               , created_at
            from {{ .Table }}
           where id in (
                 select unnest(histogram_bounds::text::text[])
                   from pg_stats
                  where tablename='{{ .Table }}'
                    and attname='id'
                 )
           order by created_at desc
           ) t1
  ) t2
  where min_created_at < $1
  order by min_created_at desc
  limit 1;
`
	selectPastThreshold = `
select id
     , created_at
  from {{ .Table }}
 where id > $1
   and id < $2
   and created_at > $3
 order by id asc
 limit 1;
`
	selectBeforeThreshold = `
select id
     , created_at
		 , xmin::text
  from {{ .Table }}
 where id < $1
 order by id desc
 limit 1;
 `
)

func main() {
	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGTERM)
	go func() {
		<-sigs
		logger.Log("msg", "received signal, shutting down")
		cancel()
	}()

	logger.Log("event", "connect", "dbname", *database, "host", *host, "port", *port, "user", *user)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("host=%s port=%d database=%s user=%s", *host, *port, *database, *user))
	if err != nil {
		kingpin.Fatalf("failed to connect to database: %v", err)
	}

	var targetTime time.Time
	if err := conn.QueryRow(ctx, fmt.Sprintf("select '%s'::timestamp;", *targetTimeString)).Scan(&targetTime); err != nil {
		kingpin.Fatalf("invalid timestamp for target time: %s", err.Error())
	}

	thresholds := struct {
		MinID, MaxID               string
		MinCreatedAt, MaxCreatedAt time.Time
	}{}

	{
		sql, err := renderSQL("selectThresholds", selectThresholds, struct{ Table string }{*table})
		if err != nil {
			kingpin.Fatalf(err.Error())
		}

		err = conn.QueryRow(ctx, sql, *targetTimeString).Scan(
			&thresholds.MinID, &thresholds.MinCreatedAt,
			&thresholds.MaxID, &thresholds.MaxCreatedAt,
		)
		if err != nil {
			kingpin.Fatalf(err.Error())
		}
	}

	logger.Log("event", "found_thresholds",
		"min_id", thresholds.MinID, "min_created_at", thresholds.MinCreatedAt,
		"max_id", thresholds.MaxID, "max_created_at", thresholds.MaxCreatedAt)

	var (
		exceededID        string
		exceededCreatedAt time.Time
	)

	{
		sql, err := renderSQL("selectPastThreshold", selectPastThreshold, struct{ Table string }{*table})
		if err != nil {
			kingpin.Fatalf(err.Error())
		}

		if err = conn.QueryRow(ctx, sql, thresholds.MinID, thresholds.MaxID, *targetTimeString).
			Scan(&exceededID, &exceededCreatedAt); err != nil {
			kingpin.Fatalf(err.Error())
		}
	}

	logger.Log("event", "first_past_threshold",
		"exceeded_id", exceededID,
		"exceeded_created_at", exceededCreatedAt,
		"exceeded_by", exceededCreatedAt.Sub(targetTime))

	var (
		beforeID        string
		beforeXMin      string
		beforeCreatedAt time.Time
	)

	{
		sql, err := renderSQL("selectBeforeThreshold", selectBeforeThreshold, struct{ Table string }{*table})
		if err != nil {
			kingpin.Fatalf(err.Error())
		}

		if err = conn.QueryRow(ctx, sql, exceededID).Scan(&beforeID, &beforeCreatedAt, &beforeXMin); err != nil {
			kingpin.Fatalf(err.Error())
		}
	}

	logger.Log("event", "first_before_threshold",
		"before_id", beforeID,
		"before_created_at", beforeCreatedAt,
		"before_xmin", beforeXMin,
		"before_by", targetTime.Sub(beforeCreatedAt))
}

func renderSQL(name, templateSource string, data interface{}) (string, error) {
	var buffer bytes.Buffer
	t := template.Must(template.New(name).Parse(templateSource))
	if err := t.Execute(&buffer, data); err != nil {
		return "", err
	}

	return string(buffer.Bytes()), nil
}
