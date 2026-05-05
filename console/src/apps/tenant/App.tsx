// Tenant console — end-user / tenant-admin surface.
// Bound to `--tenant-console-listen` in split-mode deployments.
//
// What this bundle DELIBERATELY excludes versus the ops bundle:
//   - Cluster (pools, OSDs, balancer, topology, drives)
//   - Tenants
//   - Monitoring
//   - Encryption (system KMS config)
//   - License
//
// The /_admin/* HTTP surface is still mounted on this listener
// server-side so tenant-scoped admin actions (e.g. add a tenant user,
// edit own bucket policy, set slug-bound OIDC) keep working — those
// handlers enforce per-tenant scope via `require_tenant_admin_access`.
// We just don't ship the UI pages that would make sense only for a
// system admin.

import { useState, useEffect } from "react";
import { Routes, Route } from "react-router-dom";
import Layout from "../../components/Layout";
import Login from "../../pages/Login";
import Dashboard from "../../pages/Dashboard";
import Buckets from "../../pages/Buckets";
import BucketDetail from "../../pages/BucketDetail";
import Objects from "../../pages/Objects";
import UsersPage from "../../pages/Users";
import Identity from "../../pages/Identity";
import IcebergCatalog from "../../pages/IcebergCatalog";
import UnityCatalog from "../../pages/UnityCatalog";
import DeltaSharing from "../../pages/DeltaSharing";
import Policies from "../../pages/Policies";
import MyAccount from "../../pages/MyAccount";

export default function TenantApp() {
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
    return <Login onLogin={handleLogin} appKind="tenant" />;
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
        <Route path="iceberg" element={<IcebergCatalog />} />
        <Route path="unity" element={<UnityCatalog />} />
        <Route path="sharing" element={<DeltaSharing />} />
      </Route>
    </Routes>
  );
}
