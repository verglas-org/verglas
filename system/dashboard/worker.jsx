/** @jsx h */
/** Stateless example Dashboard Worker backed only by a declared Query binding. */
import { BarChart, Card, Dashboard, DataTable, Grid, Metric, createDashboard, h } from 'cloudflare:workers';

export default createDashboard({
  title: 'Analytics',
  render: () => (
    <Dashboard>
      <Grid>
        <Card title="Revenue">
          <Metric label="Revenue" field="revenue" query={{ binding: 'QUERY', endpoint: 'sales_by_day', params: {} }} />
        </Card>
        <Card title="Daily sales">
          <BarChart x="day" y="revenue" query={{ binding: 'QUERY', endpoint: 'sales_by_day', params: {} }} />
        </Card>
      </Grid>
      <DataTable columns={[{ key: 'day', label: 'Day' }, { key: 'revenue', label: 'Revenue' }]} query={{ binding: 'QUERY', endpoint: 'sales_by_day', params: {} }} />
    </Dashboard>
  ),
});
