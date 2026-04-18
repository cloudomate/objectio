import { useEffect, useRef, useState } from "react";
import { RefreshCw, Pause, Play } from "lucide-react";
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import PageHeader from "../components/PageHeader";
import Card from "../components/Card";
import { cluster } from "../api/client";

// Time-series data point
interface DataPoint {
  time: string;
  timestamp: number;
  [key: string]: string | number;
}

// Metric definition for charts
interface ChartDef {
  title: string;
  type: "line" | "area" | "bar";
  keys: { metric: string; label: string; color: string }[];
  unit?: string;
}

// Parse a single numeric metric from Prometheus text
function parseMetric(text: string, name: string): number {
  const re = new RegExp(`^${name}(?:\\{[^}]*\\})?\\s+(\\S+)`, "m");
  const m = text.match(re);
  return m ? parseFloat(m[1]) : 0;
}

// Parse labeled metric (e.g. objectio_s3_requests_total{operation="GetObject"})
function parseLabeledMetric(text: string, name: string, label: string, value: string): number {
  const re = new RegExp(`^${name}\\{[^}]*${label}="${value}"[^}]*\\}\\s+(\\S+)`, "m");
  const m = text.match(re);
  return m ? parseFloat(m[1]) : 0;
}

// Chart definitions — what to plot
const CHARTS: ChartDef[] = [
  {
    title: "S3 Requests (total)",
    type: "area",
    keys: [
      { metric: "s3_get", label: "GetObject", color: "#3b82f6" },
      { metric: "s3_put", label: "PutObject", color: "#10b981" },
      { metric: "s3_delete", label: "DeleteObject", color: "#ef4444" },
      { metric: "s3_list", label: "ListObjects", color: "#f59e0b" },
    ],
  },
  {
    title: "S3 Request Latency (seconds)",
    type: "line",
    unit: "s",
    keys: [
      { metric: "latency_sum", label: "Total Latency", color: "#8b5cf6" },
    ],
  },
  {
    title: "Iceberg Operations",
    type: "bar",
    keys: [
      { metric: "iceberg_requests", label: "Requests", color: "#06b6d4" },
    ],
  },
  {
    title: "HTTP Response Codes",
    type: "area",
    keys: [
      { metric: "http_2xx", label: "2xx", color: "#10b981" },
      { metric: "http_4xx", label: "4xx", color: "#f59e0b" },
      { metric: "http_5xx", label: "5xx", color: "#ef4444" },
    ],
  },
];

const MAX_POINTS = 60; // 5 minutes at 5s interval
const POLL_INTERVAL = 5000;

export default function Monitoring() {
  const [history, setHistory] = useState<DataPoint[]>([]);
  const [raw, setRaw] = useState("");
  const [showRaw, setShowRaw] = useState(false);
  const [paused, setPaused] = useState(false);
  const prevValues = useRef<Record<string, number>>({});
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const scrape = async () => {
    try {
      const text = await cluster.metrics();
      setRaw(text);

      const now = new Date();
      const timeLabel = now.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });

      // Current counter values
      const current: Record<string, number> = {
        s3_get: parseLabeledMetric(text, "objectio_s3_requests_total", "operation", "GetObject"),
        s3_put: parseLabeledMetric(text, "objectio_s3_requests_total", "operation", "PutObject"),
        s3_delete: parseLabeledMetric(text, "objectio_s3_requests_total", "operation", "DeleteObject"),
        s3_list: parseLabeledMetric(text, "objectio_s3_requests_total", "operation", "ListObjects"),
        latency_sum: parseMetric(text, "objectio_s3_request_duration_seconds_sum"),
        iceberg_requests: parseMetric(text, "objectio_iceberg_requests_total"),
        http_2xx: parseLabeledMetric(text, "objectio_http_responses_total", "status", "2xx"),
        http_4xx: parseLabeledMetric(text, "objectio_http_responses_total", "status", "4xx"),
        http_5xx: parseLabeledMetric(text, "objectio_http_responses_total", "status", "5xx"),
      };

      // Compute rates (delta from previous scrape)
      const prev = prevValues.current;
      const point: DataPoint = { time: timeLabel, timestamp: now.getTime() };

      for (const [key, val] of Object.entries(current)) {
        if (prev[key] !== undefined) {
          point[key] = Math.max(0, val - prev[key]);
        } else {
          point[key] = 0;
        }
      }

      prevValues.current = current;

      setHistory((h) => {
        const next = [...h, point];
        return next.length > MAX_POINTS ? next.slice(-MAX_POINTS) : next;
      });
    } catch {
      // ignore scrape errors
    }
  };

  useEffect(() => {
    scrape(); // initial
    timerRef.current = setInterval(() => {
      if (!paused) scrape();
    }, POLL_INTERVAL);
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [paused]);

  const renderChart = (def: ChartDef) => {
    const ChartComponent = def.type === "bar" ? BarChart : def.type === "area" ? AreaChart : LineChart;

    return (
      <Card key={def.title} title={def.title}>
        <div className="h-52">
          <ResponsiveContainer width="100%" height="100%">
            <ChartComponent data={history} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="time"
                tick={{ fontSize: 10, fill: "#9ca3af" }}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fontSize: 10, fill: "#9ca3af" }}
                width={40}
                tickFormatter={(v: number) =>
                  def.unit === "s" ? `${v.toFixed(2)}s` : v >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(v)
                }
              />
              <Tooltip
                contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #e5e7eb" }}
                labelStyle={{ fontWeight: 600 }}
              />
              <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
              {def.keys.map(({ metric, label, color }) =>
                def.type === "area" ? (
                  <Area
                    key={metric}
                    type="monotone"
                    dataKey={metric}
                    name={label}
                    stroke={color}
                    fill={color}
                    fillOpacity={0.15}
                    strokeWidth={2}
                    dot={false}
                  />
                ) : def.type === "bar" ? (
                  <Bar key={metric} dataKey={metric} name={label} fill={color} radius={[4, 4, 0, 0]} />
                ) : (
                  <Line
                    key={metric}
                    type="monotone"
                    dataKey={metric}
                    name={label}
                    stroke={color}
                    strokeWidth={2}
                    dot={false}
                  />
                )
              )}
            </ChartComponent>
          </ResponsiveContainer>
        </div>
      </Card>
    );
  };

  // Summary stats from latest scrape
  const latest = history.length > 0 ? history[history.length - 1] : null;

  return (
    <div className="p-6">
      <PageHeader
        title="Monitoring"
        description={`Live metrics (${POLL_INTERVAL / 1000}s refresh) — ${history.length} data points`}
        action={
          <div className="flex gap-2">
            <button
              onClick={() => setPaused(!paused)}
              className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium ${
                paused ? "bg-green-100 text-green-700 hover:bg-green-200" : "bg-gray-100 hover:bg-gray-200"
              }`}
            >
              {paused ? <Play size={14} /> : <Pause size={14} />}
              {paused ? "Resume" : "Pause"}
            </button>
            <button
              onClick={() => setShowRaw(!showRaw)}
              className="px-3 py-2 bg-gray-100 rounded-lg text-sm font-medium hover:bg-gray-200"
            >
              {showRaw ? "Charts" : "Raw"}
            </button>
            <button
              onClick={scrape}
              className="flex items-center gap-2 px-3 py-2 bg-gray-100 rounded-lg text-sm font-medium hover:bg-gray-200"
            >
              <RefreshCw size={14} /> Now
            </button>
          </div>
        }
      />

      {showRaw ? (
        <pre className="bg-white rounded-xl border border-gray-200 p-6 text-xs overflow-auto max-h-[70vh] font-mono">
          {raw}
        </pre>
      ) : (
        <>
          {/* Summary cards */}
          {latest && (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
              {[
                { label: "GET/s", value: latest.s3_get, color: "text-blue-600" },
                { label: "PUT/s", value: latest.s3_put, color: "text-green-600" },
                { label: "Iceberg ops", value: latest.iceberg_requests, color: "text-cyan-600" },
                { label: "Errors", value: (Number(latest.http_4xx || 0) + Number(latest.http_5xx || 0)), color: "text-red-600" },
              ].map(({ label, value, color }) => (
                <div key={label} className="bg-white rounded-xl border border-gray-200 p-4">
                  <p className="text-xs text-gray-500">{label}</p>
                  <p className={`text-2xl font-semibold ${color}`}>
                    {typeof value === "number" ? value.toFixed(0) : value}
                  </p>
                </div>
              ))}
            </div>
          )}

          {/* Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {CHARTS.map(renderChart)}
          </div>
        </>
      )}
    </div>
  );
}
