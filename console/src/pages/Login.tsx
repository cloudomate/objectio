import { useEffect, useState } from "react";
import { KeyRound, LogIn, Lock, User, Building2 } from "lucide-react";
import wordmark from "../assets/brand/wordmark-light.svg";

interface Props {
  onLogin: (user: string, tenant: string) => void;
  /// Which bundle is rendering this Login. Lets us tweak the Account
  /// field's required/optional treatment without forcing two copies of
  /// the page. Defaults to "ops" so older callers (and the legacy
  /// single-bundle build) keep their current behavior.
  appKind?: "ops" | "tenant";
}

interface TenantSso {
  tenant: string;
  display_name: string;
  sso_enabled: boolean;
  provider_name?: string;
}

/// Console sign-in. The real auth flow is access-key + secret — Username
/// and Password fields map to those.
///
/// AWS-style tenant scoping: a `?tenant=NAME` URL query (or the Account
/// input below) scopes both AK/SK and SSO to that tenant. The SSO button
/// then shows only the tenant's own provider, not the system providers.
export default function Login({ onLogin, appKind = "ops" }: Props) {
  const tenantApp = appKind === "tenant";
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [remember, setRemember] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [oidcEnabled, setOidcEnabled] = useState(false);
  const [providers, setProviders] = useState<
    { name: string; label: string }[]
  >([]);

  // Tenant scoping. `tenantInput` is what the user typed (or pre-filled
  // from ?tenant=). `tenantSso` is the resolved server response that
  // tells us whether to show a tenant-specific SSO button. Empty input =
  // system-wide login (no tenant scoping; system SSO providers visible).
  const [tenantInput, setTenantInput] = useState("");
  const [tenantSso, setTenantSso] = useState<TenantSso | null>(null);
  const [tenantLookupError, setTenantLookupError] = useState("");

  useEffect(() => {
    fetch("/_console/api/oidc/enabled")
      .then((r) => r.json())
      .then((d) => {
        setOidcEnabled(d.enabled === true);
        setProviders(d.providers || []);
      })
      .catch(() => setOidcEnabled(false));

    const params = new URLSearchParams(window.location.search);
    const err = params.get("error");
    if (err) {
      setError(err);
      window.history.replaceState({}, "", window.location.pathname);
    }
    const t = params.get("tenant");
    if (t) {
      setTenantInput(t);
      lookupTenant(t);
    }
  }, []);

  // Resolve the tenant's SSO config. Called on URL pre-fill and on
  // input blur so we don't fire a request on every keystroke.
  const lookupTenant = async (name: string) => {
    setTenantLookupError("");
    setTenantSso(null);
    if (!name.trim()) return;
    try {
      const r = await fetch(`/_console/api/tenant/${encodeURIComponent(name)}/sso`);
      if (r.status === 404) {
        setTenantLookupError(`Tenant "${name}" not found`);
        return;
      }
      if (!r.ok) {
        setTenantLookupError(`Tenant lookup failed (${r.status})`);
        return;
      }
      setTenantSso(await r.json());
    } catch {
      setTenantLookupError("Tenant lookup failed");
    }
  };

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);

    try {
      const resp = await fetch("/_console/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          accessKey,
          secretKey,
          // Forward the account input only when supplied. Backend
          // refuses login if the typed account doesn't match the
          // tenant encoded in the access key — catches "wrong account"
          // mistakes that would otherwise silently land the user in
          // the wrong tenant scope.
          ...(tenantInput.trim() ? { account: tenantInput.trim() } : {}),
        }),
      });

      if (!resp.ok) {
        const body = await resp.text().catch(() => "");
        setError(body || "Invalid access key or secret key");
        return;
      }

      const data = await resp.json();
      onLogin(data.display_name || data.email || data.user, data.tenant || "");
    } catch {
      setError("Failed to connect to server");
    } finally {
      setLoading(false);
    }
  };

  const handleSsoLogin = (providerName?: string) => {
    const qs = new URLSearchParams();
    if (providerName) qs.set("provider", providerName);
    if (tenantSso?.tenant) qs.set("tenant", tenantSso.tenant);
    const params = qs.toString();
    window.location.href = `/_console/api/oidc/authorize${params ? `?${params}` : ""}`;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 via-white to-gray-100 flex items-center justify-center p-6">
      <div className="w-full max-w-md">
        <div className="bg-white rounded-2xl border border-gray-200 shadow-xl shadow-gray-900/5 overflow-hidden">
          <div className="p-7">
            {/* Wordmark */}
            <div className="flex flex-col items-start gap-1 mb-6">
              <img
                src={wordmark}
                alt="objectio"
                className="h-8 w-auto select-none"
                draggable={false}
              />
              <span className="text-[11px] text-gray-400 uppercase tracking-[0.12em] pl-0.5">
                Console
              </span>
            </div>

            <h1 className="text-[20px] font-semibold text-gray-900 mb-1">
              Sign in
            </h1>
            <p className="text-[12px] text-gray-500 mb-5">
              Enter your credentials to access the console.
            </p>

            {error && (
              <div className="flex items-center gap-1.5 text-[12px] text-red-600 bg-red-50 border border-red-200 px-2.5 py-1.5 rounded-lg mb-4">
                <KeyRound size={12} />
                {error}
              </div>
            )}

            <form onSubmit={handleLogin} className="space-y-3">
              {/* Account / Tenant. On the tenant console the field is
                  always meaningful (every login here is a tenant
                  login); on the ops console it's optional — empty
                  means system-admin login. */}
              <div>
                <label className="block text-[11px] font-semibold text-gray-700 mb-1">
                  Account
                  {!tenantApp && (
                    <span className="text-gray-400 font-normal"> (optional)</span>
                  )}
                </label>
                <div className="relative">
                  <Building2
                    size={13}
                    className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400"
                  />
                  <input
                    type="text"
                    value={tenantInput}
                    onChange={(e) => {
                      setTenantInput(e.target.value);
                      // Clear stale lookup until they tab out / submit.
                      setTenantSso(null);
                      setTenantLookupError("");
                    }}
                    onBlur={(e) => lookupTenant(e.target.value)}
                    placeholder={
                      tenantApp ? "your account name" : "tenant name (leave blank for system)"
                    }
                    autoFocus={tenantApp}
                    className="w-full pl-8 pr-2.5 py-2 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                </div>
                {tenantLookupError && (
                  <p className="mt-1 text-[11px] text-red-600">{tenantLookupError}</p>
                )}
                {tenantSso && (
                  <p className="mt-1 text-[11px] text-gray-500">
                    {tenantSso.display_name || tenantSso.tenant}
                    {tenantSso.sso_enabled
                      ? " · SSO available"
                      : " · password-only (no SSO configured)"}
                  </p>
                )}
              </div>

              {/* Access key */}
              <div>
                <label className="block text-[11px] font-semibold text-gray-700 mb-1">
                  Username
                </label>
                <div className="relative">
                  <User
                    size={13}
                    className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400"
                  />
                  <input
                    type="text"
                    value={accessKey}
                    onChange={(e) => setAccessKey(e.target.value)}
                    placeholder="AKIA…"
                    className="w-full pl-8 pr-2.5 py-2 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                    autoFocus={!tenantApp}
                  />
                </div>
              </div>

              {/* Secret key */}
              <div>
                <div className="flex items-center justify-between mb-1">
                  <label className="block text-[11px] font-semibold text-gray-700">
                    Password
                  </label>
                  <button
                    type="button"
                    className="text-[11px] text-blue-600 hover:text-blue-700"
                    onClick={() =>
                      alert(
                        "Recovery workflow is operator-side: regenerate access keys from the CLI (objectio-cli user create-key).",
                      )
                    }
                  >
                    Forgot?
                  </button>
                </div>
                <div className="relative">
                  <Lock
                    size={13}
                    className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400"
                  />
                  <input
                    type="password"
                    value={secretKey}
                    onChange={(e) => setSecretKey(e.target.value)}
                    placeholder="Secret access key"
                    className="w-full pl-8 pr-2.5 py-2 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                </div>
              </div>

              <label className="inline-flex items-center gap-1.5 text-[11px] text-gray-600 mt-1">
                <input
                  type="checkbox"
                  checked={remember}
                  onChange={(e) => setRemember(e.target.checked)}
                  className="h-3.5 w-3.5 accent-blue-600"
                />
                Remember connection
              </label>

              <button
                type="submit"
                disabled={loading || !accessKey || !secretKey}
                className="w-full flex items-center justify-center gap-2 px-3 py-2.5 bg-blue-600 text-white rounded-lg text-[13px] font-semibold hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
              >
                {loading ? "Connecting…" : "Connect →"}
              </button>
            </form>

            {/* SSO. Two distinct rendering modes:
              *   - Tenant scope active (Account input filled + lookup OK):
              *     show ONE button for the tenant's own SSO, never the
              *     system providers (so a tenant user can't accidentally
              *     log into the wrong account).
              *   - No tenant scope: show the system-wide provider list as
              *     before.                                              */}
            {tenantSso?.sso_enabled ? (
              <>
                <div className="relative my-5">
                  <div className="absolute inset-0 flex items-center">
                    <div className="w-full border-t border-gray-200" />
                  </div>
                  <div className="relative flex justify-center text-[10px]">
                    <span className="bg-white px-2 text-gray-400 uppercase tracking-wider">
                      Or sign in to {tenantSso.display_name || tenantSso.tenant}
                    </span>
                  </div>
                </div>

                <button
                  onClick={() => handleSsoLogin(tenantSso.provider_name)}
                  className="w-full flex items-center justify-center gap-2 px-3 py-2.5 bg-gray-50 border border-gray-200 text-gray-800 rounded-lg text-[13px] font-medium hover:bg-gray-100"
                >
                  <LogIn size={14} />
                  Continue with SSO
                </button>
              </>
            ) : (
              !tenantInput.trim() &&
              oidcEnabled && (
                <>
                  <div className="relative my-5">
                    <div className="absolute inset-0 flex items-center">
                      <div className="w-full border-t border-gray-200" />
                    </div>
                    <div className="relative flex justify-center text-[10px]">
                      <span className="bg-white px-2 text-gray-400 uppercase tracking-wider">
                        Or continue with
                      </span>
                    </div>
                  </div>

                  {providers.length <= 1 ? (
                    <button
                      onClick={() => handleSsoLogin(providers[0]?.name)}
                      className="w-full flex items-center justify-center gap-2 px-3 py-2.5 bg-gray-50 border border-gray-200 text-gray-800 rounded-lg text-[13px] font-medium hover:bg-gray-100"
                    >
                      <LogIn size={14} />
                      {providers[0]?.label || "SSO / Active Directory"}
                    </button>
                  ) : (
                    <div className="space-y-1.5">
                      {providers.map((p) => (
                        <button
                          key={p.name}
                          onClick={() => handleSsoLogin(p.name)}
                          className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-gray-50 border border-gray-200 text-gray-800 rounded-lg text-[12px] font-medium hover:bg-gray-100"
                        >
                          <LogIn size={13} />
                          {p.label}
                        </button>
                      ))}
                    </div>
                  )}
                </>
              )
            )}
          </div>
        </div>

        <p className="mt-4 text-[11px] text-gray-400 text-center">
          Need help? See the{" "}
          <a
            href="https://github.com/cloudomate/objectio"
            className="text-blue-600 hover:text-blue-700"
            target="_blank"
            rel="noreferrer"
          >
            ObjectIO docs
          </a>
          .
        </p>
      </div>
    </div>
  );
}
