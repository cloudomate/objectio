import { useEffect, useState } from "react";

export interface LicenseInfo {
  tier: "community" | "enterprise";
  licensee: string;
  issued_at: number;
  expires_at: number;
  features: string[];
  enabled_features: string[];
}

const COMMUNITY: LicenseInfo = {
  tier: "community",
  licensee: "community",
  issued_at: 0,
  expires_at: 0,
  features: [],
  enabled_features: [],
};

// Enterprise feature identifiers — must match objectio_license::Feature::as_str()
export type FeatureKey =
  | "iceberg"
  | "delta_sharing"
  | "kms"
  | "multi_tenancy"
  | "oidc"
  | "lrc";

export function allows(license: LicenseInfo | null | undefined, feature: FeatureKey): boolean {
  if (!license) return false;
  return license.enabled_features.includes(feature);
}

/**
 * Load license info from /_admin/license, cached per app session.
 * Falls back to Community if the endpoint is unreachable or unauthorized.
 */
export function useLicense(): LicenseInfo | null {
  const [license, setLicense] = useState<LicenseInfo | null>(null);
  useEffect(() => {
    let cancelled = false;
    fetch("/_admin/license", { credentials: "include" })
      .then((r) => (r.ok ? r.json() : COMMUNITY))
      .then((j) => {
        if (!cancelled) setLicense(j as LicenseInfo);
      })
      .catch(() => {
        if (!cancelled) setLicense(COMMUNITY);
      });
    return () => {
      cancelled = true;
    };
  }, []);
  return license;
}
