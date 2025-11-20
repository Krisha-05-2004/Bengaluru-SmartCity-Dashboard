console.log("App rendered!");
import React, { useState, useMemo } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
  BarChart,
  Bar,
} from "recharts";
import Papa from "papaparse";

export default function App() {
  const sampleKPIs = {
    population: "12.3M",
    avgAQI: 78,
    avgTemp: "28°C",
    trafficIndex: 6.8,
  };

  const sampleTraffic = [
    { day: "Mon", congestIndex: 6.1 },
    { day: "Tue", congestIndex: 6.5 },
    { day: "Wed", congestIndex: 7.0 },
    { day: "Thu", congestIndex: 7.4 },
    { day: "Fri", congestIndex: 8.2 },
    { day: "Sat", congestIndex: 5.2 },
    { day: "Sun", congestIndex: 4.9 },
  ];

  const samplePower = [
    { hour: "00", usage: 120 },
    { hour: "03", usage: 100 },
    { hour: "06", usage: 130 },
    { hour: "09", usage: 210 },
    { hour: "12", usage: 260 },
    { hour: "15", usage: 240 },
    { hour: "18", usage: 300 },
    { hour: "21", usage: 220 },
  ];

  const sampleModal = [
    { mode: "Private", share: 62 },
    { mode: "Public", share: 25 },
    { mode: "Two-wheeler", share: 10 },
    { mode: "Walk/Cycle", share: 3 },
  ];

  const sampleWeather = {
    today: { temp: "29°C", rainfall_mm: 0.4, condition: "Partly Cloudy" },
    tomorrow: { temp: "30°C", rainfall_mm: 0.0, condition: "Sunny" },
  };

  const [trafficData, setTrafficData] = useState(sampleTraffic);
  const [powerData, setPowerData] = useState(samplePower);
  const [modalData, setModalData] = useState(sampleModal);
  const [kpis] = useState(sampleKPIs);
  const [weather] = useState(sampleWeather);
  const [uploadedFileName, setUploadedFileName] = useState(null);

  const avgCongestion = useMemo(() => {
    const sum = trafficData.reduce((s, r) => s + (r.congestIndex || 0), 0);
    return (sum / trafficData.length).toFixed(2);
  }, [trafficData]);

  function handleFileUpload(e) {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploadedFileName(file.name);

    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      complete: (results) => {
        const rows = results.data;

        if (rows[0]?.day && rows[0]?.congestIndex) {
          setTrafficData(
            rows.map((r) => ({
              day: r.day,
              congestIndex: Number(r.congestIndex),
            }))
          );
        } else if (rows[0]?.hour && rows[0]?.usage) {
          setPowerData(
            rows.map((r) => ({
              hour: r.hour,
              usage: Number(r.usage),
            }))
          );
        } else if (rows[0]?.mode && rows[0]?.share) {
          setModalData(
            rows.map((r) => ({
              mode: r.mode,
              share: Number(r.share),
            }))
          );
        }
      },
    });
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6 font-sans">
      <h1 className="text-3xl font-bold mb-4">SmartCity — Bengaluru Dashboard</h1>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="p-4 bg-white shadow rounded">Population: {kpis.population}</div>
        <div className="p-4 bg-white shadow rounded">Avg AQI: {kpis.avgAQI}</div>
        <div className="p-4 bg-white shadow rounded">Avg Temp: {kpis.avgTemp}</div>
        <div className="p-4 bg-white shadow rounded">Traffic Index: {avgCongestion}</div>
      </div>

      <div className="bg-white p-4 rounded shadow mb-6">
        <h2 className="font-semibold mb-2">Weekly Traffic Congestion</h2>
        <div style={{ height: 300 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={trafficData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="day" />
              <YAxis />
              <Tooltip />
              <Line dataKey="congestIndex" stroke="#3b82f6" strokeWidth={3} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <label>
          <input type="file" className="hidden" id="csvUpload" onChange={handleFileUpload} />
        </label>
        <button
          onClick={() => document.getElementById("csvUpload").click()}
          className="mt-3 px-3 py-1 bg-indigo-600 text-white rounded"
        >
          Upload CSV
        </button>
        <div className="text-sm mt-2 text-gray-500">
          {uploadedFileName ? `Loaded: ${uploadedFileName}` : "No CSV uploaded"}
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-white p-4 rounded shadow">
          <h3 className="font-semibold mb-2">Power Usage</h3>
          <div style={{ height: 240 }}>
            <ResponsiveContainer>
              <AreaChart data={powerData}>
                <XAxis dataKey="hour" />
                <YAxis />
                <Tooltip />
                <Area dataKey="usage" stroke="#22c55e" fill="#86efac" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-white p-4 rounded shadow">
          <h3 className="font-semibold mb-2">Transport Modal Split</h3>
          <div style={{ height: 240 }}>
            <ResponsiveContainer>
              <BarChart layout="vertical" data={modalData}>
                <XAxis type="number" />
                <YAxis type="category" dataKey="mode" />
                <Tooltip />
                <Bar dataKey="share" fill="#a855f7" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
