package realtime

// Authorizer validates permissions for realtime operations.
type Authorizer interface {
	// AuthorizeJoin returns nil if userID is allowed to join roomID, or an error otherwise.
	AuthorizeJoin(userID, roomID string) error
	// AuthorizePublish returns nil if userID is allowed to publish to roomID, or an error otherwise.
	AuthorizePublish(userID, roomID string) error
	// AuthorizePrivateSend returns nil if senderID is allowed to send a private message to recipientID, or an error otherwise.
	AuthorizePrivateSend(senderID, recipientID string) error
}

// AllowAllAuthorizer accepts all operations.
type AllowAllAuthorizer struct{}

// AuthorizeJoin always allows any user to join any room.
func (AllowAllAuthorizer) AuthorizeJoin(userID, roomID string) error {
	return nil
}

// AuthorizePublish always allows any user to publish to any room.
func (AllowAllAuthorizer) AuthorizePublish(userID, roomID string) error {
	return nil
}

// AuthorizePrivateSend always allows any user to send a private message to any recipient.
func (AllowAllAuthorizer) AuthorizePrivateSend(senderID, recipientID string) error {
	return nil
}
