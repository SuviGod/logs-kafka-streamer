#!/bin/sh
echo creating datasource
curl --user admin:admin  -H 'Accept: application/json' -H 'Content-Type: application/json; charset=UTF-8' -X POST --data @datasource.json http://localhost:3000/api/datasources
echo datasource created
echo creating dashboard
curl --user admin:admin  -H 'Accept: application/json' -H 'Content-Type: application/json; charset=UTF-8' -X POST --data @dashboard.json http://localhost:3000/api/dashboards/db
echo dashboard created