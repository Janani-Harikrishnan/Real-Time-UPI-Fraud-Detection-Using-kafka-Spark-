import React, { useState, useEffect } from 'react';
import { LineChart, Line, BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import './App.css';

// Custom colors for the Pie Chart
const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#a29bfe'];

function App() {
  const [viewMode, setViewMode] = useState('live'); 
  const [liveData, setLiveData] = useState(null);
  const [historyData, setHistoryData] = useState([]);

  // Fetch Live Data
  useEffect(() => {
    let interval;
    if (viewMode === 'live') {
      const fetchLive = async () => {
        try {
          const res = await fetch('http://localhost:5000/api/latest-batch');
          const data = await res.json();
          if (data.status === 'success') setLiveData(data);
        } catch (error) {
          console.error("Error fetching live data:", error);
        }
      };
      fetchLive();
      interval = setInterval(fetchLive, 2000);
    }
    return () => clearInterval(interval);
  }, [viewMode]);

  // Fetch History Data (Used for the Line Chart)
  useEffect(() => {
    const fetchHistory = async () => {
      try {
        const res = await fetch('http://localhost:5000/api/last-five-batches');
        const data = await res.json();
        if (data.status === 'success') setHistoryData(data.history.reverse()); // Reverse for chronological order
      } catch (error) {
        console.error("Error fetching history:", error);
      }
    };
    fetchHistory();
    // Refresh history every 2 seconds to keep charts animated
    const histInterval = setInterval(fetchHistory, 2000); 
    return () => clearInterval(histInterval);
  }, []);

  // --- DATA PROCESSING FOR CHARTS ---

  // 1. Process data for Device Pie Chart
  const getDeviceStats = () => {
    if (!liveData || !liveData.data) return [];
    const counts = {};
    liveData.data.forEach(txn => {
      counts[txn.device_type] = (counts[txn.device_type] || 0) + 1;
    });
    return Object.keys(counts).map(key => ({ name: key, value: counts[key] }));
  };

  // 2. Process data for Location Bar Chart
  const getLocationStats = () => {
    if (!liveData || !liveData.data) return [];
    const counts = {};
    liveData.data.forEach(txn => {
      counts[txn.location] = (counts[txn.location] || 0) + 1;
    });
    // Sort and grab top 5 locations
    return Object.keys(counts)
      .map(key => ({ name: key, frauds: counts[key] }))
      .sort((a, b) => b.frauds - a.frauds)
      .slice(0, 5);
  };

  // --- RENDERING HELPERS ---
  const renderTable = (frauds) => {
    if (!frauds || frauds.length === 0) return <p>No data available.</p>;
    const columns = Object.keys(frauds[0]).filter(key => key !== "pipeline_detected_at" && key !== "batch_id" && key !== "total_batch_txns");
    return (
      <table className="data-table">
        <thead>
          <tr>{columns.map(col => <th key={col}>{col.toUpperCase()}</th>)}</tr>
        </thead>
        <tbody>
          {frauds.map((row, idx) => (
            <tr key={idx}>
              {columns.map(col => (
                <td key={col}>{typeof row[col] === 'number' ? row[col].toFixed(2) : String(row[col])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    );
  };

  return (
    <div className="App">
      <header className="header">
        <h1>🚨 UPI Fraud Detection Console</h1>
        <div className="nav-buttons">
          <button className={viewMode === 'live' ? 'active' : ''} onClick={() => setViewMode('live')}>Live Monitor</button>
          <button className={viewMode === 'history' ? 'active' : ''} onClick={() => setViewMode('history')}>Last 5 Batches</button>
        </div>
      </header>

      <main className="content">
        {viewMode === 'live' && (
          <div className="live-view">
            {liveData ? (
              <>
                <div className="stats-cards">
                  <div className="card"><h3>Batch ID</h3><h2>{liveData.batch_id}</h2></div>
                  <div className="card"><h3>Total Transactions</h3><h2>{liveData.total_txns}</h2></div>
                  <div className="card danger"><h3>Potential Frauds</h3><h2>{liveData.fraud_count}</h2></div>
                </div>

                {/* --- NEW VISUAL ANALYTICS SECTION --- */}
                <div className="charts-container" style={{ display: 'flex', gap: '20px', marginBottom: '30px', height: '300px' }}>
                  
                  {/* Line Chart: History */}
                  <div className="chart-box" style={{ flex: 2, background: '#1e1e1e', padding: '15px', borderRadius: '8px' }}>
                    <h3 style={{ marginTop: 0, color: '#aaa', fontSize: '14px' }}>Fraud Velocity (Last 5 Batches)</h3>
                    <ResponsiveContainer width="100%" height="90%">
                      <LineChart data={historyData}>
                        <XAxis dataKey="batch_id" stroke="#888" />
                        <YAxis stroke="#888" />
                        <Tooltip background="#333" />
                        <Line type="monotone" dataKey="frauds.length" name="Frauds Detected" stroke="#ff4757" strokeWidth={3} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>

                  {/* Pie Chart: Devices */}
                  <div className="chart-box" style={{ flex: 1, background: '#1e1e1e', padding: '15px', borderRadius: '8px' }}>
                    <h3 style={{ marginTop: 0, color: '#aaa', fontSize: '14px' }}>Compromised Devices</h3>
                    <ResponsiveContainer width="100%" height="90%">
                      <PieChart>
                        <Pie data={getDeviceStats()} innerRadius={60} outerRadius={80} paddingAngle={5} dataKey="value">
                          {getDeviceStats().map((entry, index) => <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />)}
                        </Pie>
                        <Tooltip />
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>

                  {/* Bar Chart: Locations */}
                  <div className="chart-box" style={{ flex: 1, background: '#1e1e1e', padding: '15px', borderRadius: '8px' }}>
                    <h3 style={{ marginTop: 0, color: '#aaa', fontSize: '14px' }}>Top Risk Locations</h3>
                    <ResponsiveContainer width="100%" height="90%">
                      <BarChart data={getLocationStats()}>
                        <XAxis dataKey="name" stroke="#888" tick={{fontSize: 10}} />
                        <Tooltip />
                        <Bar dataKey="frauds" fill="#FFBB28" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>

                </div>
                {/* -------------------------------------- */}

                <h3>🔴 Live Fraud Details</h3>
                {renderTable(liveData.data)}
              </>
            ) : (
              <h2>Waiting for Spark Pipeline...</h2>
            )}
          </div>
        )}

        {viewMode === 'history' && (
          <div className="history-view">
            <h2>📜 History: Last 5 Fraud Events</h2>
            {historyData.slice().reverse().map((batch, index) => (
              <div key={index} className="history-batch">
                <div className="history-header">
                  <h3>Batch {batch.batch_id}</h3>
                  <p>Transactions: {batch.total_txns} | Frauds: {batch.frauds.length}</p>
                </div>
                {renderTable(batch.frauds)}
              </div>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}

export default App;