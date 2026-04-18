import { useEffect, useState } from "react";
import { KeyRound, LogIn, Lock, User } from "lucide-react";
import wordmark from "../assets/brand/wordmark-light.svg";

interface Props {
  onLogin: (user: string, tenant: string) => void;
}

type Environment = "production" | "laboratory";

/// Console sign-in. The real auth flow is access-key + secret — Username
/// and Password fields map to those. The "Cluster Endpoint" field is
/// display-only on the deployed console (the client is already talking
/// to the endpoint that served this page), but we keep it visible to
/// reassure operators they're about to drive the intended cluster.
export default function Login({ onLogin }: Props) {
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [environment, setEnvironment] = useState<Environment>("production");
  const [remember, setRemember] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [oidcEnabled, setOidcEnabled] = useState(false);
  const [providers, setProviders] = useState<
    { name: string; label: string }[]
  >([]);

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
  }, []);

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);

    try {
      const resp = await fetch("/_console/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ accessKey, secretKey }),
      });

      if (!resp.ok) {
        setError("Invalid access key or secret key");
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
    const params = providerName ? `?provider=${encodeURIComponent(providerName)}` : "";
    window.location.href = `/_console/api/oidc/authorize${params}`;
  };

  const host =
    typeof window !== "undefined"
      ? window.location.host
      : "cluster.local";

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

            {/* Env toggle */}
            <div className="flex items-center justify-end mb-5">
              <div className="inline-flex p-0.5 rounded-lg bg-gray-100">
                {(["production", "laboratory"] as Environment[]).map((env) => (
                  <button
                    key={env}
                    onClick={() => setEnvironment(env)}
                    className={`px-3 py-1 text-[11px] font-medium rounded-md capitalize transition-colors ${
                      environment === env
                        ? "bg-white text-gray-900 shadow-sm"
                        : "text-gray-500 hover:text-gray-700"
                    }`}
                  >
                    {env}
                  </button>
                ))}
              </div>
            </div>

            <h1 className="text-[20px] font-semibold text-gray-900 mb-1">
              Connect to Cluster
            </h1>
            <p className="text-[12px] text-gray-500 mb-5">
              Enter your endpoint and credentials to access the console.
            </p>

            {error && (
              <div className="flex items-center gap-1.5 text-[12px] text-red-600 bg-red-50 border border-red-200 px-2.5 py-1.5 rounded-lg mb-4">
                <KeyRound size={12} />
                {error}
              </div>
            )}

            <form onSubmit={handleLogin} className="space-y-3">
              {/* Endpoint — display-only for console that's already connected */}
              <div>
                <label className="block text-[11px] font-semibold text-gray-700 mb-1">
                  Cluster Endpoint
                </label>
                <div className="relative">
                  <span className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400">
                    <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
                      <rect x="2" y="3" width="12" height="4" rx="1" />
                      <rect x="2" y="9" width="12" height="4" rx="1" />
                      <circle cx="4.5" cy="5" r="0.5" fill="currentColor" />
                      <circle cx="4.5" cy="11" r="0.5" fill="currentColor" />
                    </svg>
                  </span>
                  <input
                    readOnly
                    value={host}
                    className="w-full pl-8 pr-2.5 py-2 bg-gray-50 border border-gray-200 rounded-lg text-[13px] font-mono text-gray-700 focus:outline-none"
                    aria-readonly="true"
                  />
                </div>
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
                    autoFocus
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

            {oidcEnabled && (
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
            )}
          </div>

          <div className="px-7 py-2.5 bg-gray-50 border-t border-gray-100 text-[10px] text-gray-400 text-center">
            {environment === "laboratory"
              ? "Laboratory mode — ephemeral cluster, non-persistent state"
              : "Production mode — live cluster"}
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
