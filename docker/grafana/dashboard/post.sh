echo creating datasource
curl -H 'Accept: application/json' -H 'Content-Type: application/json; charset=UTF-8' -X POST --data @datasource.json http://localhost:3000/api/datasources
echo datasource created
echo creating dashboard
curl -H 'Accept: application/json' -H 'Content-Type: application/json; charset=UTF-8' -X POST --data @dashboard.json http://localhost:3000/api/dashboards/db
echo dashboard created