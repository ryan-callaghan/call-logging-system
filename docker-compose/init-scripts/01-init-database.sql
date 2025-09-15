-- Create database schema for call logging system
-- This script runs automatically when PostgreSQL container starts

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    uuid UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone_number VARCHAR(20),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(100),
    role VARCHAR(50) DEFAULT 'user',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Event metadata for quick lookups and joins
CREATE TABLE event_metadata (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) UNIQUE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    user_id INTEGER REFERENCES users(id),
    source_service VARCHAR(100),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    indexed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB
);

-- Call sessions for grouping related events
CREATE TABLE call_sessions (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) UNIQUE NOT NULL,
    user_id INTEGER REFERENCES users(id),
    started_at TIMESTAMP WITH TIME ZONE,
    ended_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    call_count INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'active'
);

-- System health and monitoring
CREATE TABLE system_health (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    last_check TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    response_time_ms INTEGER,
    error_message TEXT,
    metadata JSONB
);

-- Create indexes for better performance
CREATE INDEX idx_event_metadata_user_timestamp ON event_metadata(user_id, timestamp);
CREATE INDEX idx_event_metadata_type ON event_metadata(event_type);
CREATE INDEX idx_event_metadata_processed ON event_metadata(processed_at);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_call_sessions_user ON call_sessions(user_id);

-- Insert sample users for testing
INSERT INTO users (username, email, phone_number, first_name, last_name, department, role) VALUES 
('john.doe', 'john.doe@company.com', '+1-555-0101', 'John', 'Doe', 'Sales', 'user'),
('jane.smith', 'jane.smith@company.com', '+1-555-0102', 'Jane', 'Smith', 'Support', 'user'),
('admin.user', 'admin@company.com', '+1-555-0100', 'Admin', 'User', 'IT', 'admin');

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update updated_at
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Log successful initialization
INSERT INTO system_health (service_name, status, error_message) 
VALUES ('database_init', 'success', 'Database schema initialized successfully');