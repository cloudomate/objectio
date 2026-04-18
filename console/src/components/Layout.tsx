import { NavLink, Outlet } from "react-router-dom";
import {
  LayoutDashboard,
  Archive,
  Users,
  UserCircle,
  Shield,
  Fingerprint,
  Table2,
  Share2,
  Activity,
  HardDrive,
  Box,
  Layers,
  Building2,
  KeyRound,
  ScrollText,
  LogOut,
  Lock,
} from "lucide-react";
import { useLicense, allows, type FeatureKey } from "../lib/license";
import wordmark from "../assets/brand/wordmark-light.svg";

interface NavItem {
  to: string;
  icon: typeof LayoutDashboard;
  label: string;
  adminOnly?: boolean;
  /// If set, this nav item requires the named Enterprise feature. When the
  /// feature is locked the item still appears but is disabled and links to
  /// /license instead.
  feature?: FeatureKey;
}

const nav: NavItem[] = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/buckets", icon: Archive, label: "Buckets" },
  { to: "/objects", icon: Box, label: "Object Browser" },
  { to: "/pools", icon: Layers, label: "Storage Pools", adminOnly: true },
  { to: "/tenants", icon: Building2, label: "Tenants", adminOnly: true, feature: "multi_tenancy" },
  { to: "/users", icon: Users, label: "Users" },
  { to: "/policies", icon: Shield, label: "Policies" },
  { to: "/identity", icon: Fingerprint, label: "Identity", feature: "oidc" },
  { to: "/iceberg", icon: Table2, label: "Tables", feature: "iceberg" },
  { to: "/sharing", icon: Share2, label: "Table Sharing", feature: "delta_sharing" },
  { to: "/monitoring", icon: Activity, label: "Monitoring", adminOnly: true },
  { to: "/drives", icon: HardDrive, label: "Nodes & Drives", adminOnly: true },
  { to: "/topology", icon: Layers, label: "Topology", adminOnly: true },
  { to: "/encryption", icon: KeyRound, label: "Encryption", adminOnly: true, feature: "kms" },
  { to: "/license", icon: ScrollText, label: "License", adminOnly: true },
];

interface Props {
  user?: string;
  tenant?: string;
  onLogout?: () => void;
}

export default function Layout({ user, tenant, onLogout }: Props) {
  const isSystemAdmin = !tenant;
  const license = useLicense();
  const visibleNav = nav.filter((n) => !n.adminOnly || isSystemAdmin);

  return (
    <div className="flex h-screen">
      {/* Sidebar */}
      <aside className="w-52 bg-white border-r border-gray-200 flex flex-col">
        <div className="px-3 py-3 border-b border-gray-200">
          <div className="flex flex-col items-start gap-0.5">
            <img
              src={wordmark}
              alt="objectio"
              className="h-6 w-auto select-none"
              draggable={false}
            />
            <span className="text-[10px] text-gray-400 uppercase tracking-wider pl-0.5">
              Console
            </span>
          </div>
        </div>
        <nav className="flex-1 p-2 space-y-px overflow-y-auto">
          {visibleNav.map(({ to, icon: Icon, label, feature }) => {
            // A feature-gated nav item still renders when locked so users
            // can see what Enterprise unlocks — but the link points to
            // /license and shows a padlock instead of navigating to the
            // gated page (which would just 403 anyway).
            const locked = feature !== undefined && !allows(license, feature);
            const target = locked ? "/license" : to;
            return (
              <NavLink
                key={to}
                to={target}
                end={to === "/"}
                title={locked ? `Requires Enterprise license — click to manage` : undefined}
                className={({ isActive }) =>
                  `flex items-center gap-2.5 px-2.5 py-1.5 rounded-lg text-[12px] font-medium transition-colors ${
                    isActive && !locked
                      ? "bg-blue-50 text-blue-700"
                      : locked
                        ? "text-gray-400 hover:bg-gray-50"
                        : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                  }`
                }
              >
                <Icon size={15} />
                <span className="flex-1 min-w-0 truncate">{label}</span>
                {locked && <Lock size={11} className="text-gray-400 shrink-0" />}
              </NavLink>
            );
          })}
        </nav>
        <div className="px-2 py-2 border-t border-gray-200">
          {user && onLogout ? (
            // The user panel doubles as the entry point to My Account — clicking
            // the name/tenant block opens the profile page; Sign-out is a
            // separate icon so it's always one click away.
            <div className="flex items-center justify-between gap-1">
              <NavLink
                to="/account"
                className={({ isActive }) =>
                  `flex items-center gap-2 flex-1 min-w-0 px-1.5 py-1 rounded-lg transition-colors ${
                    isActive
                      ? "bg-blue-50 text-blue-700"
                      : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                  }`
                }
                title="My account"
              >
                <UserCircle size={15} className="shrink-0" />
                <div className="min-w-0 truncate">
                  <span className="block text-[11px] font-medium truncate">{user}</span>
                  {tenant && (
                    <span className="block text-[10px] text-purple-500 truncate">{tenant}</span>
                  )}
                </div>
              </NavLink>
              <button
                onClick={onLogout}
                className="text-gray-400 hover:text-gray-700 p-1.5 rounded-lg hover:bg-gray-50"
                title="Sign out"
              >
                <LogOut size={13} />
              </button>
            </div>
          ) : (
            <span className="text-[10px] text-gray-400">ObjectIO v0.1.0</span>
          )}
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto bg-gray-50">
        <Outlet />
      </main>
    </div>
  );
}
