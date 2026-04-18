import { useState, useEffect } from "react";
import { Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import Login from "./pages/Login";
import Dashboard from "./pages/Dashboard";
import Buckets from "./pages/Buckets";
import BucketDetail from "./pages/BucketDetail";
import Objects from "./pages/Objects";
import UsersPage from "./pages/Users";
import Identity from "./pages/Identity";
import Pools from "./pages/Pools";
import Tenants from "./pages/Tenants";
import IcebergCatalog from "./pages/IcebergCatalog";
import DeltaSharing from "./pages/DeltaSharing";
import Monitoring from "./pages/Monitoring";
import Drives from "./pages/Drives";
import Policies from "./pages/Policies";
import Encryption from "./pages/Encryption";
import LicensePage from "./pages/License";
import Topology from "./pages/Topology";
import MyAccount from "./pages/MyAccount";

export default function App() {
  const [user, setUser] = useState<string | null>(null);
  const [tenant, setTenant] = useState("");
  const [checking, setChecking] = useState(true);

  // Check for existing session on load
  useEffect(() => {
    fetch("/_console/api/session")
      .then((r) => {
        if (r.ok) return r.json();
        throw new Error("no session");
      })
      .then((data) => {
        // Prefer the human-readable display_name; fall back to email,
        // then the raw user_id for older sessions / edge cases.
        setUser(data.display_name || data.email || data.user);
        setTenant(data.tenant || "");
      })
      .catch(() => setUser(null))
      .finally(() => setChecking(false));
  }, []);

  const handleLogin = (u: string, t: string) => {
    setUser(u);
    setTenant(t);
  };

  const handleLogout = async () => {
    await fetch("/_console/api/logout", { method: "POST" });
    setUser(null);
    setTenant("");
  };

  if (checking) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <p className="text-sm text-gray-400">Loading...</p>
      </div>
    );
  }

  if (!user) {
    return <Login onLogin={handleLogin} />;
  }

  const isSystemAdmin = !tenant;

  return (
    <Routes>
      <Route element={<Layout user={user} tenant={tenant} onLogout={handleLogout} />}>
        <Route index element={<Dashboard />} />
        <Route path="account" element={<MyAccount />} />
        <Route path="buckets" element={<Buckets />} />
        <Route path="buckets/:bucketName" element={<BucketDetail />} />
        <Route path="objects" element={<Objects />} />
        <Route path="users" element={<UsersPage />} />
        <Route path="policies" element={<Policies />} />
        <Route path="identity" element={<Identity />} />
        {isSystemAdmin && <Route path="pools" element={<Pools />} />}
        {isSystemAdmin && <Route path="tenants" element={<Tenants />} />}
        <Route path="iceberg" element={<IcebergCatalog />} />
        <Route path="sharing" element={<DeltaSharing />} />
        {isSystemAdmin && <Route path="monitoring" element={<Monitoring />} />}
        {isSystemAdmin && <Route path="drives" element={<Drives />} />}
        {isSystemAdmin && <Route path="encryption" element={<Encryption />} />}
        {isSystemAdmin && <Route path="license" element={<LicensePage />} />}
        {isSystemAdmin && <Route path="topology" element={<Topology />} />}
      </Route>
    </Routes>
  );
}
