import { useEffect, useState } from "react";
import { X, Shield } from "lucide-react";

/// Modal-overlay editor for an entity-level Unity Catalog policy.
///
/// Mirrors the IAM bridge: same JSON shape as `/_admin/policies` (BucketPolicy
/// with PascalCase keys). Empty body means "no policy attached" → clears.
interface Props {
  /// Heading displayed in the modal header (e.g. "policy on prod_analytics.marketing").
  scope: string;
  /// Loader: pulls existing policy JSON from the server. May resolve to {}.
  load: () => Promise<{ policy: unknown }>;
  /// Saver: PUTs the parsed JSON back. Throws on validation/server errors.
  save: (policy: unknown) => Promise<void>;
  onClose: () => void;
}

const EXAMPLE = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Action": ["unity:DeleteTable"],
      "Resource": ["*"]
    }
  ]
}`;

export default function PolicyEditor({ scope, load, save, onClose }: Props) {
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    load()
      .then((r) => {
        if (!alive) return;
        const empty =
          r.policy === null ||
          r.policy === undefined ||
          (typeof r.policy === "object" &&
            Object.keys(r.policy as object).length === 0);
        setText(empty ? "" : JSON.stringify(r.policy, null, 2));
      })
      .catch((e) => alive && setError(String(e)))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [load]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const onSave = async () => {
    setError(null);
    let parsed: unknown;
    try {
      // Empty editor means "remove the policy". Server's PUT validator
      // refuses bare `{}` (BucketPolicy requires Statement), so send a
      // zero-statement policy — observably equivalent to no policy
      // (ImplicitDeny everywhere; no contribution to evaluation).
      parsed =
        text.trim() === ""
          ? { Version: "2012-10-17", Statement: [] }
          : JSON.parse(text);
    } catch (e) {
      setError(`Invalid JSON: ${e}`);
      return;
    }
    setBusy(true);
    try {
      await save(parsed);
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      className="fixed inset-0 bg-black/30 z-50 flex items-center justify-center p-4"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[85vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between px-5 py-3 border-b border-gray-200">
          <div className="flex items-center gap-2">
            <Shield size={16} className="text-indigo-500" />
            <div>
              <h2 className="text-[14px] font-semibold text-gray-900">
                Policy
              </h2>
              <div className="text-[12px] text-gray-500 font-mono">{scope}</div>
            </div>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-700 p-1 rounded hover:bg-gray-50"
          >
            <X size={14} />
          </button>
        </div>

        <div className="px-5 py-3 text-[12px] text-gray-600 border-b border-gray-100">
          IAM-shape JSON. Evaluated <span className="font-medium">after</span>{" "}
          attached IAM policies, before child entities. Deny at any level wins.
          Empty saves clear the policy.
        </div>

        {loading ? (
          <div className="p-8 text-center text-[12px] text-gray-400">
            Loading…
          </div>
        ) : (
          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder={EXAMPLE}
            spellCheck={false}
            className="flex-1 px-5 py-3 font-mono text-[12px] focus:outline-none resize-none min-h-[280px]"
          />
        )}

        {error && (
          <div className="px-5 py-2 bg-red-50 border-t border-red-100 text-[12px] text-red-700 font-mono break-all">
            {error}
          </div>
        )}

        <div className="flex justify-end gap-2 px-5 py-3 border-t border-gray-200 bg-gray-50 rounded-b-xl">
          <button
            onClick={onClose}
            className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-white"
          >
            Cancel
          </button>
          <button
            onClick={onSave}
            disabled={busy || loading}
            className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800 disabled:opacity-50"
          >
            {busy ? "Saving…" : "Save"}
          </button>
        </div>
      </div>
    </div>
  );
}
