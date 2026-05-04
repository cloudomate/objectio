import { useEffect, useState } from "react";
import {
  Users as UsersIcon,
  Plus,
  Trash2,
  ChevronRight,
  X,
  UserPlus,
} from "lucide-react";
import { groups as groupsApi, users as usersApi, type Group, type User } from "../api/client";

/// Groups management panel — sibling to the Users table on the same page.
///
/// Wraps the `groups` API: list, create, delete, plus add/remove members.
/// Membership lives on the group (`member_user_ids` is part of `GroupMeta`),
/// so expanding a row triggers no extra request — we already have it.
export default function GroupsPanel() {
  const [groups, setGroups] = useState<Group[]>([]);
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");

  const [expanded, setExpanded] = useState<string | null>(null);
  const [addingTo, setAddingTo] = useState<string | null>(null);
  const [pickUserId, setPickUserId] = useState("");

  const load = () => {
    setLoading(true);
    setError(null);
    groupsApi
      .list()
      .then((r) => setGroups(r.groups || []))
      .catch((e) => {
        setGroups([]);
        setError(String(e));
      })
      .finally(() => setLoading(false));
  };

  useEffect(load, []);
  useEffect(() => {
    usersApi.list().then((r) => setUsers(r.users || [])).catch(() => setUsers([]));
  }, []);

  const create = async () => {
    if (!newName.trim()) return;
    try {
      await groupsApi.create(newName.trim());
      setNewName("");
      setShowCreate(false);
      load();
    } catch (e) {
      setError(String(e));
    }
  };

  const remove = async (g: Group) => {
    if (!confirm(`Delete group "${g.group_name}"?`)) return;
    try {
      await groupsApi.delete(g.group_id);
      load();
    } catch (e) {
      setError(String(e));
    }
  };

  const addMember = async (groupId: string) => {
    if (!pickUserId) return;
    try {
      await groupsApi.addMember(groupId, pickUserId);
      setAddingTo(null);
      setPickUserId("");
      load();
    } catch (e) {
      setError(String(e));
    }
  };

  const removeMember = async (groupId: string, userId: string) => {
    try {
      await groupsApi.removeMember(groupId, userId);
      load();
    } catch (e) {
      setError(String(e));
    }
  };

  const userLabel = (id: string) =>
    users.find((u) => u.user_id === id)?.display_name || id.slice(0, 8);

  return (
    <div>
      <div className="flex items-center justify-between mb-3">
        <span className="text-[12px] text-gray-500">
          IAM groups — attach a policy to a group and every member inherits it.
        </span>
        <button
          onClick={() => setShowCreate(true)}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
        >
          <Plus size={14} /> Create Group
        </button>
      </div>

      {error && (
        <div className="mb-3 px-3 py-2 bg-red-50 border border-red-200 rounded-lg text-[12px] text-red-700 flex items-start justify-between gap-2">
          <span className="font-mono break-all">{error}</span>
          <button
            onClick={() => setError(null)}
            className="text-red-400 hover:text-red-700"
          >
            <X size={12} />
          </button>
        </div>
      )}

      {showCreate && (
        <div className="mb-3 bg-white rounded-xl border border-gray-200 p-3 flex gap-2 items-center">
          <input
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            placeholder="group-name"
            className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
            onKeyDown={(e) => e.key === "Enter" && create()}
            autoFocus
          />
          <button
            onClick={create}
            className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
          >
            Create
          </button>
          <button
            onClick={() => {
              setShowCreate(false);
              setNewName("");
            }}
            className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
          >
            Cancel
          </button>
        </div>
      )}

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Group
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-24">
                Members
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                ARN
              </th>
              <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              <tr>
                <td
                  colSpan={4}
                  className="px-4 py-6 text-center text-[12px] text-gray-400"
                >
                  Loading…
                </td>
              </tr>
            ) : groups.length === 0 ? (
              <tr>
                <td
                  colSpan={4}
                  className="px-4 py-6 text-center text-[12px] text-gray-400"
                >
                  No groups. Create one to start.
                </td>
              </tr>
            ) : (
              groups.map((g) => (
                <RowFragment
                  key={g.group_id}
                  group={g}
                  expanded={expanded === g.group_id}
                  toggle={() =>
                    setExpanded(expanded === g.group_id ? null : g.group_id)
                  }
                  addingTo={addingTo}
                  setAddingTo={setAddingTo}
                  pickUserId={pickUserId}
                  setPickUserId={setPickUserId}
                  users={users}
                  userLabel={userLabel}
                  addMember={addMember}
                  removeMember={removeMember}
                  remove={remove}
                />
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

interface RowProps {
  group: Group;
  expanded: boolean;
  toggle: () => void;
  addingTo: string | null;
  setAddingTo: (v: string | null) => void;
  pickUserId: string;
  setPickUserId: (v: string) => void;
  users: User[];
  userLabel: (id: string) => string;
  addMember: (gid: string) => void;
  removeMember: (gid: string, uid: string) => void;
  remove: (g: Group) => void;
}

// Row + optional expanded panel. Pulled out so the parent stays readable.
function RowFragment(p: RowProps) {
  const nonMembers = p.users.filter(
    (u) => !p.group.member_user_ids.includes(u.user_id),
  );
  return (
    <>
      <tr className="hover:bg-gray-50 group">
        <td className="px-4 py-2.5">
          <button
            onClick={p.toggle}
            className="flex items-center gap-2 w-full text-left"
          >
            <ChevronRight
              size={12}
              className={`text-gray-400 transition-transform ${p.expanded ? "rotate-90" : ""}`}
            />
            <UsersIcon size={14} className="text-purple-500" />
            <span className="text-[13px] font-medium">{p.group.group_name}</span>
          </button>
        </td>
        <td className="px-4 py-2.5 text-[12px] text-gray-600">
          {p.group.member_user_ids.length}
        </td>
        <td className="px-4 py-2.5 text-[11px] text-gray-500 font-mono truncate max-w-[280px]">
          {p.group.arn}
        </td>
        <td className="px-4 py-2.5 text-right">
          <button
            onClick={() => p.remove(p.group)}
            className="text-gray-400 hover:text-red-600 p-1 opacity-0 group-hover:opacity-100 transition-opacity"
          >
            <Trash2 size={13} />
          </button>
        </td>
      </tr>
      {p.expanded && (
        <tr>
          <td colSpan={4} className="bg-gray-50 px-4 py-3">
            <div className="flex items-center justify-between mb-2">
              <h4 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Members
              </h4>
              {p.addingTo === p.group.group_id ? (
                <div className="flex gap-1.5 items-center">
                  <select
                    value={p.pickUserId}
                    onChange={(e) => p.setPickUserId(e.target.value)}
                    className="px-2 py-1 border border-gray-300 rounded text-[12px]"
                  >
                    <option value="">— pick user —</option>
                    {nonMembers.map((u) => (
                      <option key={u.user_id} value={u.user_id}>
                        {u.display_name}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={() => p.addMember(p.group.group_id)}
                    className="px-2 py-1 bg-gray-900 text-white rounded text-[11px]"
                  >
                    Add
                  </button>
                  <button
                    onClick={() => {
                      p.setAddingTo(null);
                      p.setPickUserId("");
                    }}
                    className="px-2 py-1 text-gray-500 text-[11px]"
                  >
                    Cancel
                  </button>
                </div>
              ) : (
                <button
                  onClick={() => p.setAddingTo(p.group.group_id)}
                  className="text-[11px] text-blue-600 hover:text-blue-800 flex items-center gap-1"
                >
                  <UserPlus size={11} /> Add Member
                </button>
              )}
            </div>
            {p.group.member_user_ids.length === 0 ? (
              <p className="text-[12px] text-gray-400">No members</p>
            ) : (
              <div className="space-y-1.5">
                {p.group.member_user_ids.map((uid) => (
                  <div
                    key={uid}
                    className="flex items-center justify-between bg-white rounded-lg px-3 py-1.5 border border-gray-200"
                  >
                    <div className="flex items-center gap-2 text-[12px]">
                      <UsersIcon size={11} className="text-gray-400" />
                      <span>{p.userLabel(uid)}</span>
                      <span className="text-[10px] font-mono text-gray-400">
                        {uid.slice(0, 8)}
                      </span>
                    </div>
                    <button
                      onClick={() => p.removeMember(p.group.group_id, uid)}
                      className="text-red-400 hover:text-red-600"
                      title="Remove from group"
                    >
                      <Trash2 size={12} />
                    </button>
                  </div>
                ))}
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}
