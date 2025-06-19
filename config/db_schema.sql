-- Database Schema for Configuration Management
-- Recommended for production deployment with multiple plants

CREATE TABLE data_processing_config (
    config_id SERIAL PRIMARY KEY,
    plant_name VARCHAR(100) NOT NULL,
    config_name VARCHAR(100) NOT NULL,
    description TEXT,
    local_file_path VARCHAR(255) NOT NULL,
    bucket_name VARCHAR(100) NOT NULL,
    directory_s3 VARCHAR(255) NOT NULL,
    default_year INTEGER,
    default_month INTEGER,
    default_day INTEGER,
    patterns JSONB NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_active_config UNIQUE (plant_name, config_name) 
        WHERE (is_active = TRUE)
);

CREATE TABLE file_mappings (
    mapping_id SERIAL PRIMARY KEY,
    config_id INTEGER NOT NULL REFERENCES data_processing_config(config_id) 
        ON DELETE CASCADE,
    file_name VARCHAR(255) NOT NULL,
    variable_name VARCHAR(100) NOT NULL,
    file_type VARCHAR(20) DEFAULT 'parquet',
    is_required BOOLEAN DEFAULT TRUE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_file_mapping UNIQUE (config_id, file_name)
);

-- Indexes for better performance
CREATE INDEX idx_config_plant ON data_processing_config(plant_name);
CREATE INDEX idx_config_active ON data_processing_config(is_active);
CREATE INDEX idx_mappings_config ON file_mappings(config_id);
