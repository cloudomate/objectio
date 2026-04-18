//! User and access key storage

use crate::error::AuthError;
use crate::user::{AccessKey, KeyStatus, User, UserStatus};
use parking_lot::RwLock;
use std::collections::HashMap;

/// In-memory user and access key store
///
/// In production, this would be backed by the metadata service
pub struct UserStore {
    /// Users indexed by user_id
    users: RwLock<HashMap<String, User>>,
    /// Access keys indexed by access_key_id
    keys: RwLock<HashMap<String, AccessKey>>,
    /// Map from user_id to their access_key_ids
    user_keys: RwLock<HashMap<String, Vec<String>>>,
}

impl Default for UserStore {
    fn default() -> Self {
        Self::new()
    }
}

impl UserStore {
    /// Create a new empty user store
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            keys: RwLock::new(HashMap::new()),
            user_keys: RwLock::new(HashMap::new()),
        }
    }

    /// Create a user store with a default admin user
    pub fn with_admin(admin_name: &str) -> Self {
        let store = Self::new();

        // Create admin user
        let admin = User::new(admin_name);
        let user_id = admin.user_id.clone();

        store.users.write().insert(user_id.clone(), admin);

        // Create admin access key
        let key = AccessKey::generate(&user_id);
        let key_id = key.access_key_id.clone();

        store.keys.write().insert(key_id.clone(), key);
        store.user_keys.write().insert(user_id, vec![key_id]);

        store
    }

    // =========== User Operations ===========

    /// Create a new user
    pub fn create_user(&self, display_name: &str) -> Result<User, AuthError> {
        let mut users = self.users.write();

        // Check if user with same name exists
        if users.values().any(|u| u.display_name == display_name) {
            return Err(AuthError::UserAlreadyExists(display_name.to_string()));
        }

        let user = User::new(display_name);
        let user_clone = user.clone();
        users.insert(user.user_id.clone(), user);
        self.user_keys
            .write()
            .insert(user_clone.user_id.clone(), Vec::new());

        Ok(user_clone)
    }

    /// Get user by ID
    pub fn get_user(&self, user_id: &str) -> Result<User, AuthError> {
        self.users
            .read()
            .get(user_id)
            .cloned()
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))
    }

    /// Get user by display name
    pub fn get_user_by_name(&self, display_name: &str) -> Result<User, AuthError> {
        self.users
            .read()
            .values()
            .find(|u| u.display_name == display_name)
            .cloned()
            .ok_or_else(|| AuthError::UserNotFound(display_name.to_string()))
    }

    /// List all users
    pub fn list_users(&self) -> Vec<User> {
        self.users.read().values().cloned().collect()
    }

    /// Update user status
    pub fn update_user_status(&self, user_id: &str, status: UserStatus) -> Result<(), AuthError> {
        let mut users = self.users.write();
        let user = users
            .get_mut(user_id)
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;
        user.status = status;
        Ok(())
    }

    /// Delete user (marks as deleted, doesn't remove)
    pub fn delete_user(&self, user_id: &str) -> Result<(), AuthError> {
        // Mark user as deleted
        self.update_user_status(user_id, UserStatus::Deleted)?;

        // Deactivate all user's keys
        let key_ids: Vec<String> = self
            .user_keys
            .read()
            .get(user_id)
            .cloned()
            .unwrap_or_default();

        let mut keys = self.keys.write();
        for key_id in key_ids {
            if let Some(key) = keys.get_mut(&key_id) {
                key.status = KeyStatus::Inactive;
            }
        }

        Ok(())
    }

    // =========== Access Key Operations ===========

    /// Create a new access key for a user
    pub fn create_access_key(&self, user_id: &str) -> Result<AccessKey, AuthError> {
        // Verify user exists and is active
        let user = self.get_user(user_id)?;
        if !user.is_active() {
            return Err(AuthError::UserSuspended);
        }

        let key = AccessKey::generate(user_id);
        let key_clone = key.clone();

        self.keys.write().insert(key.access_key_id.clone(), key);
        self.user_keys
            .write()
            .entry(user_id.to_string())
            .or_default()
            .push(key_clone.access_key_id.clone());

        Ok(key_clone)
    }

    /// Get access key by ID
    pub fn get_access_key(&self, access_key_id: &str) -> Result<AccessKey, AuthError> {
        self.keys
            .read()
            .get(access_key_id)
            .cloned()
            .ok_or_else(|| AuthError::AccessKeyNotFound(access_key_id.to_string()))
    }

    /// List access keys for a user
    pub fn list_access_keys(&self, user_id: &str) -> Vec<AccessKey> {
        let key_ids = self
            .user_keys
            .read()
            .get(user_id)
            .cloned()
            .unwrap_or_default();

        let keys = self.keys.read();
        key_ids
            .iter()
            .filter_map(|id| keys.get(id).cloned())
            .collect()
    }

    /// Update access key status
    pub fn update_access_key_status(
        &self,
        access_key_id: &str,
        status: KeyStatus,
    ) -> Result<(), AuthError> {
        let mut keys = self.keys.write();
        let key = keys
            .get_mut(access_key_id)
            .ok_or_else(|| AuthError::AccessKeyNotFound(access_key_id.to_string()))?;
        key.status = status;
        Ok(())
    }

    /// Delete access key
    pub fn delete_access_key(&self, access_key_id: &str) -> Result<(), AuthError> {
        let key = self.keys.write().remove(access_key_id);
        if let Some(key) = key {
            // Remove from user_keys
            if let Some(keys) = self.user_keys.write().get_mut(&key.user_id) {
                keys.retain(|id| id != access_key_id);
            }
            Ok(())
        } else {
            Err(AuthError::AccessKeyNotFound(access_key_id.to_string()))
        }
    }

    // =========== Lookup for Auth ===========

    /// Look up access key and associated user for authentication
    pub fn lookup_for_auth(&self, access_key_id: &str) -> Result<(AccessKey, User), AuthError> {
        let key = self.get_access_key(access_key_id)?;

        if !key.is_active() {
            return Err(AuthError::AccessKeyInactive);
        }

        let user = self.get_user(&key.user_id)?;

        if !user.is_active() {
            return Err(AuthError::UserSuspended);
        }

        Ok((key, user))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_user() {
        let store = UserStore::new();
        let user = store.create_user("testuser").unwrap();
        assert_eq!(user.display_name, "testuser");
        assert!(user.is_active());
    }

    #[test]
    fn test_duplicate_user() {
        let store = UserStore::new();
        store.create_user("testuser").unwrap();
        let result = store.create_user("testuser");
        assert!(matches!(result, Err(AuthError::UserAlreadyExists(_))));
    }

    #[test]
    fn test_create_access_key() {
        let store = UserStore::new();
        let user = store.create_user("testuser").unwrap();
        let key = store.create_access_key(&user.user_id).unwrap();
        assert!(key.is_active());
        assert_eq!(key.user_id, user.user_id);
    }

    #[test]
    fn test_lookup_for_auth() {
        let store = UserStore::new();
        let user = store.create_user("testuser").unwrap();
        let key = store.create_access_key(&user.user_id).unwrap();

        let (found_key, found_user) = store.lookup_for_auth(&key.access_key_id).unwrap();
        assert_eq!(found_key.access_key_id, key.access_key_id);
        assert_eq!(found_user.user_id, user.user_id);
    }

    #[test]
    fn test_with_admin() {
        let store = UserStore::with_admin("admin");
        let users = store.list_users();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].display_name, "admin");

        let keys = store.list_access_keys(&users[0].user_id);
        assert_eq!(keys.len(), 1);
    }
}
