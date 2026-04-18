import { useEffect, useState } from "react";
import { KeyRound, LogIn } from "lucide-react";
import wordmark from "../assets/brand/wordmark-light.svg";

interface Props {
  onLogin: (user: string, tenant: string) => void;
}

export default function Login({ onLogin }: Props) {
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [oidcEnabled, setOidcEnabled] = useState(false);
  const [providers, setProviders] = useState<
    { name: string; label: string }[]
  >([]);
  const [showAkSk, setShowAkSk] = useState(false);

  // Check if OIDC is available
  useEffect(() => {
    fetch("/_console/api/oidc/enabled")
      .then((r) => r.json())
      .then((d) => {
        setOidcEnabled(d.enabled === true);
        setProviders(d.providers || []);
      })
      .catch(() => setOidcEnabled(false));

    // Check for error from OIDC callback
    const params = new URLSearchParams(window.location.search);
    const err = params.get("error");
    if (err) {
      setError(err);
      // Clean URL
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

  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center">
      <div className="w-full max-w-xs">
        <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm">
          <div className="flex items-center gap-2.5 mb-5">
            <img
              src={wordmark}
              alt="objectio"
              className="h-7 w-auto select-none"
              draggable={false}
            />
            <span className="text-[11px] text-gray-400 uppercase tracking-wider">
              Console
            </span>
          </div>

          {error && (
            <div className="flex items-center gap-1.5 text-[12px] text-red-600 bg-red-50 border border-red-200 px-2.5 py-1.5 rounded-lg mb-4">
              <KeyRound size={12} />
              {error}
            </div>
          )}

          {/* SSO Login Buttons */}
          {oidcEnabled && (
            <>
              {providers.length <= 1 ? (
                <button
                  onClick={() => handleSsoLogin(providers[0]?.name)}
                  className="w-full flex items-center justify-center gap-2 px-3 py-2.5 bg-gray-900 text-white rounded-lg text-[13px] font-medium hover:bg-gray-800 mb-3"
                >
                  <LogIn size={15} />
                  {providers[0]?.label || "Sign in with SSO"}
                </button>
              ) : (
                <div className="space-y-1.5 mb-3">
                  {providers.map((p) => (
                    <button
                      key={p.name}
                      onClick={() => handleSsoLogin(p.name)}
                      className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                    >
                      <LogIn size={14} />
                      {p.label}
                    </button>
                  ))}
                </div>
              )}

              {!showAkSk && (
                <button
                  onClick={() => setShowAkSk(true)}
                  className="w-full text-center text-[11px] text-gray-400 hover:text-gray-600 py-1"
                >
                  Or sign in with access key
                </button>
              )}
            </>
          )}

          {/* AK/SK Login Form */}
          {(!oidcEnabled || showAkSk) && (
            <form onSubmit={handleLogin} className="space-y-3">
              {oidcEnabled && (
                <div className="relative my-3">
                  <div className="absolute inset-0 flex items-center">
                    <div className="w-full border-t border-gray-200" />
                  </div>
                  <div className="relative flex justify-center text-[10px]">
                    <span className="bg-white px-2 text-gray-400">
                      Access Key Login
                    </span>
                  </div>
                </div>
              )}
              <div>
                <label className="block text-[12px] font-medium text-gray-700 mb-1">
                  Access Key
                </label>
                <input
                  type="text"
                  value={accessKey}
                  onChange={(e) => setAccessKey(e.target.value)}
                  placeholder="AKIA..."
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  autoFocus={!oidcEnabled}
                />
              </div>
              <div>
                <label className="block text-[12px] font-medium text-gray-700 mb-1">
                  Secret Key
                </label>
                <input
                  type="password"
                  value={secretKey}
                  onChange={(e) => setSecretKey(e.target.value)}
                  placeholder="Secret access key"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                />
              </div>

              <button
                type="submit"
                disabled={loading || !accessKey || !secretKey}
                className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-blue-600 text-white rounded-lg text-[13px] font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? "Signing in..." : "Sign In"}
              </button>
            </form>
          )}

          <p className="mt-3 text-[11px] text-gray-400 text-center">
            {oidcEnabled
              ? "Sign in with your organization account or access key."
              : "Use your IAM access key and secret key to sign in."}
          </p>
        </div>
      </div>
    </div>
  );
}
