import { useEffect, useMemo } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  Network,
  HardDrive as HardDriveIcon,
  Layers as LayersIcon,
  Scale,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import Tabs from "../components/Tabs";
import Topology from "./Topology";
import Drives from "./Drives";
import Pools from "./Pools";
import Balancing from "./Balancing";

type TabKey = "topology" | "drives" | "pools" | "balancing";

const TABS: Array<{ key: TabKey; label: string; icon: React.ReactNode }> = [
  { key: "topology", label: "Topology", icon: <Network size={14} /> },
  { key: "drives", label: "Nodes & Drives", icon: <HardDriveIcon size={14} /> },
  { key: "pools", label: "Storage Pools", icon: <LayersIcon size={14} /> },
  { key: "balancing", label: "Balancing", icon: <Scale size={14} /> },
];

/// Unified /cluster page. Tabs map to URL params (`/cluster/:tab`) so
/// deep links to a specific view still work — the old `/topology` /
/// `/drives` / `/pools` routes redirect here via App.tsx.
export default function Cluster() {
  const nav = useNavigate();
  const { tab: urlTab } = useParams<{ tab?: string }>();
  const active: TabKey = useMemo(() => {
    const valid = TABS.map((t) => t.key);
    return (valid.includes(urlTab as TabKey) ? urlTab : "topology") as TabKey;
  }, [urlTab]);

  useEffect(() => {
    // Normalise the URL so refresh lands on the same tab.
    if (!urlTab) {
      nav("/cluster/topology", { replace: true });
    }
  }, [urlTab, nav]);

  return (
    <div className="p-4">
      <PageHeader
        title="Cluster"
        description="Topology, storage layout, and placement-group balancing."
      />
      <div className="mt-2 mb-3">
        <Tabs
          variant="underline"
          active={active}
          onChange={(k) => nav(`/cluster/${k}`)}
          tabs={TABS}
        />
      </div>
      <div className="bg-transparent">
        {active === "topology" && <Topology embedded />}
        {active === "drives" && <Drives embedded />}
        {active === "pools" && <Pools embedded />}
        {active === "balancing" && <Balancing />}
      </div>
    </div>
  );
}
