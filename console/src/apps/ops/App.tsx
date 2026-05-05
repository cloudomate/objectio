// Ops console — full system-admin surface.
// Bound to `--ops-console-listen` in split-mode deployments. The bundle
// imports every page including pools/OSDs/balancer/license/tenants —
// pages that should NEVER be reachable from the public-facing tenant
// console (which ships from a separate bundle).
//
// In legacy single-port mode the same App.tsx (root level) drives the
// SPA; this ops variant only differs by being explicit about the
// "always system admin" assumption.

import { useState, useEffect } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import Layout from "../../components/Layout";
import Login from "../../pages/Login";
import Dashboard from "../../pages/Dashboard";
import Buckets from "../../pages/Buckets";
import BucketDetail from "../../pages/BucketDetail";
import Objects from "../../pages/Objects";
import UsersPage from "../../pages/Users";
import Identity from "../../pages/Identity";
import Tenants from "../../pages/Tenants";
import IcebergCatalog from "../../pages/IcebergCatalog";
import UnityCatalog from "../../pages/UnityCatalog";
import DeltaSharing from "../../pages/DeltaSharing";
import Monitoring from "../../pages/Monitoring";
import Policies from "../../pages/Policies";
import Encryption from "../../pages/Encryption";
import LicensePage from "../../pages/License";
import Cluster from "../../pages/Cluster";
import PoolPlacement from "../../pages/PoolPlacement";
import MyAccount from "../../pages/MyAccount";

export default function OpsApp() {
  const [user, setUser] = useState<string | null>(null);
  const [tenant, setTenant] = useState("");
  const [checking, setChecking] = useState(true);

  useEffect(() => {
    fetch("/_console/api/session")
      .then((r) => {
        if (r.ok) return r.json();
        throw new Error("no session");
      })
      .then((data) => {
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
    return <Login onLogin={handleLogin} appKind="ops" />;
  }

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
        <Route path="tenants" element={<Tenants />} />
        <Route path="iceberg" element={<IcebergCatalog />} />
        <Route path="unity" element={<UnityCatalog />} />
        <Route path="sharing" element={<DeltaSharing />} />
        <Route path="monitoring" element={<Monitoring />} />
        <Route path="encryption" element={<Encryption />} />
        <Route path="license" element={<LicensePage />} />
        <Route path="cluster" element={<Cluster />} />
        <Route path="cluster/pools/:name" element={<PoolPlacement />} />
        <Route path="cluster/:tab" element={<Cluster />} />
        <Route path="pools" element={<Navigate to="/cluster/pools" replace />} />
        <Route path="drives" element={<Navigate to="/cluster/drives" replace />} />
        <Route path="topology" element={<Navigate to="/cluster/topology" replace />} />
      </Route>
    </Routes>
  );
}
