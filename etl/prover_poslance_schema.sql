-- Czech Parliament Database Schema
-- Designed for PostgreSQL 14+

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- For full-text search

-- Drop tables if they exist (for fresh setup)
DROP TABLE IF EXISTS vote_records CASCADE;
DROP TABLE IF EXISTS voting_sessions CASCADE;
DROP TABLE IF EXISTS mp_committee_memberships CASCADE;
DROP TABLE IF EXISTS bill_authors CASCADE;
DROP TABLE IF EXISTS bills CASCADE;
DROP TABLE IF EXISTS parliament_sessions CASCADE;
DROP TABLE IF EXISTS committees CASCADE;
DROP TABLE IF EXISTS mps CASCADE;
DROP TABLE IF EXISTS persons CASCADE;
DROP TABLE IF EXISTS parties CASCADE;
DROP TABLE IF EXISTS constituencies CASCADE;
DROP TABLE IF EXISTS electoral_periods CASCADE;
DROP TABLE IF EXISTS data_sync_log CASCADE;

-- Electoral periods (volební období)
CREATE TABLE electoral_periods (
    id SERIAL PRIMARY KEY,
    period_number INTEGER NOT NULL UNIQUE,
    start_date DATE NOT NULL,
    end_date DATE,
    description TEXT,
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Constituencies (kraje/obvody)
CREATE TABLE constituencies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(10),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Political parties
CREATE TABLE parties (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    short_name VARCHAR(20),
    color_hex VARCHAR(7), -- For UI display
    founded_date DATE,
    website VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Persons (all people in the system)
CREATE TABLE persons (
    id INTEGER PRIMARY KEY, -- Using original PSP ID
    title_before VARCHAR(50),
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    title_after VARCHAR(50),
    birth_date DATE,
    death_date DATE,
    gender CHAR(1) CHECK (gender IN ('M', 'F')),
    full_name VARCHAR(255) GENERATED ALWAYS AS (
        COALESCE(title_before || ' ', '') || 
        first_name || ' ' || 
        last_name || 
        COALESCE(' ' || title_after, '')
    ) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- MPs (current and historical)
CREATE TABLE mps (
    id INTEGER PRIMARY KEY, -- Using original PSP poslanec ID
    person_id INTEGER NOT NULL REFERENCES persons(id),
    constituency_id INTEGER REFERENCES constituencies(id),
    party_id INTEGER REFERENCES parties(id),
    electoral_period_id INTEGER NOT NULL REFERENCES electoral_periods(id),
    
    -- Contact information
    email VARCHAR(255),
    phone VARCHAR(50),
    office_phone VARCHAR(50),
    fax VARCHAR(50),
    website VARCHAR(255),
    facebook VARCHAR(255),
    
    -- Address
    street VARCHAR(255),
    city VARCHAR(100),
    postal_code VARCHAR(10),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    start_date DATE,
    end_date DATE,
    
    -- Photo
    photo_url VARCHAR(500),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(person_id, electoral_period_id)
);

-- Committees and other organs
CREATE TABLE committees (
    id INTEGER PRIMARY KEY, -- Using original PSP organ ID
    name_cz VARCHAR(255) NOT NULL,
    name_en VARCHAR(255),
    short_name VARCHAR(50),
    committee_type VARCHAR(50), -- 'committee', 'subcommittee', 'parliament', etc.
    parent_committee_id INTEGER REFERENCES committees(id),
    start_date DATE,
    end_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- MP committee memberships
CREATE TABLE mp_committee_memberships (
    id SERIAL PRIMARY KEY,
    mp_id INTEGER NOT NULL REFERENCES mps(id),
    committee_id INTEGER NOT NULL REFERENCES committees(id),
    role VARCHAR(50), -- 'member', 'chairman', 'vice-chairman'
    start_date DATE,
    end_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(mp_id, committee_id, start_date)
);

-- Parliament sessions (schůze)
CREATE TABLE parliament_sessions (
    id SERIAL PRIMARY KEY,
    committee_id INTEGER NOT NULL REFERENCES committees(id),
    session_number INTEGER NOT NULL,
    start_datetime TIMESTAMP,
    end_datetime TIMESTAMP,
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(committee_id, session_number)
);

-- Bills and documents (tisky)
CREATE TABLE bills (
    id INTEGER PRIMARY KEY, -- Using original PSP tisk ID
    committee_id INTEGER REFERENCES committees(id),
    bill_number VARCHAR(50),
    title VARCHAR(500) NOT NULL,
    description TEXT,
    own_number VARCHAR(50),
    bill_type VARCHAR(100),
    status VARCHAR(100),
    submitted_date DATE,
    collection_number VARCHAR(50),
    collection_year INTEGER,
    url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bill authors (many-to-many relationship)
CREATE TABLE bill_authors (
    id SERIAL PRIMARY KEY,
    bill_id INTEGER NOT NULL REFERENCES bills(id),
    mp_id INTEGER NOT NULL REFERENCES mps(id),
    is_primary_author BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(bill_id, mp_id)
);

-- Voting sessions (jednotlivá hlasování)
CREATE TABLE voting_sessions (
    id INTEGER PRIMARY KEY, -- Using original PSP hlasovani ID
    committee_id INTEGER NOT NULL REFERENCES committees(id),
    parliament_session_id INTEGER REFERENCES parliament_sessions(id),
    session_number INTEGER,
    vote_number INTEGER NOT NULL,
    agenda_item INTEGER,
    vote_date DATE NOT NULL,
    vote_time TIME,
    
    -- Results
    votes_for INTEGER DEFAULT 0,
    votes_against INTEGER DEFAULT 0,
    abstentions INTEGER DEFAULT 0,
    did_not_vote INTEGER DEFAULT 0,
    present_count INTEGER DEFAULT 0,
    quorum_met BOOLEAN,
    
    -- Vote details
    vote_type VARCHAR(50),
    result VARCHAR(50), -- 'passed', 'failed', etc.
    title_long TEXT,
    title_short VARCHAR(255),
    
    -- Related bill
    bill_id INTEGER REFERENCES bills(id),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(committee_id, session_number, vote_number)
);

-- Individual vote records
CREATE TABLE vote_records (
    id SERIAL PRIMARY KEY,
    voting_session_id INTEGER NOT NULL REFERENCES voting_sessions(id),
    mp_id INTEGER NOT NULL REFERENCES mps(id),
    vote_result CHAR(1) NOT NULL CHECK (vote_result IN ('A', 'N', 'Z', '@', 'M', 'X', '0')),
    -- A=ano/yes, N=ne/no, Z=zdržel se/abstain, @=nepřihlášen/not registered, 
    -- M=omluven/excused, X=nehlasoval/did not vote, 0=nezaznamenán/not recorded
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(voting_session_id, mp_id)
);

-- Data synchronization log
CREATE TABLE data_sync_log (
    id SERIAL PRIMARY KEY,
    sync_type VARCHAR(50) NOT NULL, -- 'mps', 'votes', 'bills', etc.
    file_name VARCHAR(255),
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    sync_status VARCHAR(20) DEFAULT 'running', -- 'running', 'completed', 'failed'
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    
    INDEX(sync_type, started_at)
);

-- Create indexes for performance
CREATE INDEX idx_persons_name ON persons(last_name, first_name);
CREATE INDEX idx_persons_full_name_gin ON persons USING gin(full_name gin_trgm_ops);

CREATE INDEX idx_mps_person ON mps(person_id);
CREATE INDEX idx_mps_constituency ON mps(constituency_id);
CREATE INDEX idx_mps_party ON mps(party_id);
CREATE INDEX idx_mps_period ON mps(electoral_period_id);
CREATE INDEX idx_mps_active ON mps(is_active) WHERE is_active = true;

CREATE INDEX idx_vote_records_session ON vote_records(voting_session_id);
CREATE INDEX idx_vote_records_mp ON vote_records(mp_id);
CREATE INDEX idx_vote_records_result ON vote_records(vote_result);

CREATE INDEX idx_voting_sessions_date ON voting_sessions(vote_date);
CREATE INDEX idx_voting_sessions_committee ON voting_sessions(committee_id);
CREATE INDEX idx_voting_sessions_bill ON voting_sessions(bill_id);

CREATE INDEX idx_bills_date ON bills(submitted_date);
CREATE INDEX idx_bills_status ON bills(status);
CREATE INDEX idx_bills_title_gin ON bills USING gin(title gin_trgm_ops);

CREATE INDEX idx_committees_active ON committees(is_active) WHERE is_active = true;
CREATE INDEX idx_committee_memberships_mp ON mp_committee_memberships(mp_id);
CREATE INDEX idx_committee_memberships_active ON mp_committee_memberships(is_active) WHERE is_active = true;

-- Create views for common queries
CREATE VIEW v_current_mps AS
SELECT 
    m.id,
    p.full_name,
    p.first_name,
    p.last_name,
    p.birth_date,
    pt.name as party_name,
    pt.short_name as party_short,
    pt.color_hex as party_color,
    c.name as constituency_name,
    m.email,
    m.phone,
    m.website,
    m.photo_url,
    m.start_date,
    ep.period_number,
    ep.description as period_description
FROM mps m
JOIN persons p ON m.person_id = p.id
LEFT JOIN parties pt ON m.party_id = pt.id  
LEFT JOIN constituencies c ON m.constituency_id = c.id
JOIN electoral_periods ep ON m.electoral_period_id = ep.id
WHERE m.is_active = true AND ep.is_active = true;

-- MP voting statistics view
CREATE VIEW v_mp_voting_stats AS
SELECT 
    m.id as mp_id,
    p.full_name,
    pt.short_name as party,
    COUNT(vr.id) as total_votes,
    COUNT(CASE WHEN vr.vote_result = 'A' THEN 1 END) as votes_for,
    COUNT(CASE WHEN vr.vote_result = 'N' THEN 1 END) as votes_against,
    COUNT(CASE WHEN vr.vote_result = 'Z' THEN 1 END) as abstentions,
    COUNT(CASE WHEN vr.vote_result IN ('@', 'M', 'X', '0') THEN 1 END) as absent,
    ROUND(
        (COUNT(CASE WHEN vr.vote_result IN ('A', 'N', 'Z') THEN 1 END)::numeric / 
         NULLIF(COUNT(vr.id), 0)) * 100, 2
    ) as attendance_rate,
    MAX(vs.vote_date) as last_vote_date
FROM mps m
JOIN persons p ON m.person_id = p.id
LEFT JOIN parties pt ON m.party_id = pt.id
LEFT JOIN vote_records vr ON m.id = vr.mp_id
LEFT JOIN voting_sessions vs ON vr.voting_session_id = vs.id
WHERE m.is_active = true
GROUP BY m.id, p.full_name, pt.short_name;

-- Committee membership view
CREATE VIEW v_mp_committees AS
SELECT 
    m.id as mp_id,
    p.full_name as mp_name,
    c.name_cz as committee_name,
    c.short_name as committee_short,
    mcm.role,
    mcm.start_date,
    mcm.end_date,
    mcm.is_active
FROM mp_committee_memberships mcm
JOIN mps m ON mcm.mp_id = m.id
JOIN persons p ON m.person_id = p.id
JOIN committees c ON mcm.committee_id = c.id
WHERE mcm.is_active = true;

-- Recent voting sessions view
CREATE VIEW v_recent_votes AS
SELECT 
    vs.id,
    vs.vote_date,
    vs.vote_time,
    vs.title_short,
    vs.title_long,
    vs.votes_for,
    vs.votes_against,
    vs.abstentions,
    vs.result,
    c.name_cz as committee_name,
    b.title as bill_title,
    b.bill_number
FROM voting_sessions vs
LEFT JOIN committees c ON vs.committee_id = c.id
LEFT JOIN bills b ON vs.bill_id = b.id
ORDER BY vs.vote_date DESC, vs.vote_time DESC;

-- Functions for data maintenance
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add update triggers
CREATE TRIGGER update_persons_updated_at BEFORE UPDATE ON persons
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_mps_updated_at BEFORE UPDATE ON mps
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_parties_updated_at BEFORE UPDATE ON parties
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_bills_updated_at BEFORE UPDATE ON bills
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert some initial data
INSERT INTO electoral_periods (period_number, start_date, end_date, description, is_active) VALUES
(9, '2021-10-08', NULL, '9. volební období (2021-)', true),
(8, '2017-10-20', '2021-10-07', '8. volební období (2017-2021)', false),
(7, '2013-10-25', '2017-10-19', '7. volební období (2013-2017)', false);

INSERT INTO parties (name, short_name, color_hex, is_active) VALUES
('Občanská demokratická strana', 'ODS', '#0066CC', true),
('ANO 2011', 'ANO', '#EC407A', true),
('Piráti', 'Piráti', '#000000', true),
('Svoboda a přímá demokracie', 'SPD', '#DC2626', true),
('Starostové a nezávislí', 'STAN', '#FFA500', true),
('Komunistická strana Čech a Moravy', 'KSČM', '#B91C1C', true),
('Česká strana sociálně demokratická', 'ČSSD', '#F97316', true),
('TOP 09', 'TOP 09', '#8B5CF6', true),
('Křesťanská a demokratická unie', 'KDU-ČSL', '#10B981', true);

INSERT INTO constituencies (name, code, region) VALUES
('Praha', 'PHA', 'Praha'),
('Jihočeský kraj', 'JCK', 'České Budějovice'),
('Jihomoravský kraj', 'JMK', 'Brno'),
('Karlovarský kraj', 'KVK', 'Karlovy Vary'),
('Královéhradecký kraj', 'KHK', 'Hradec Králové'),
('Liberecký kraj', 'LBK', 'Liberec'),
('Moravskoslezský kraj', 'MSK', 'Ostrava'),
('Olomoucký kraj', 'OLK', 'Olomouc'),
('Pardubický kraj', 'PAK', 'Pardubice'),
('Plzeňský kraj', 'PLK', 'Plzeň'),
('Středočeský kraj', 'STK', 'Praha'),
('Ústecký kraj', 'ULK', 'Ústí nad Labem'),
('Vysočina', 'VYS', 'Jihlava'),
('Zlínský kraj', 'ZLK', 'Zlín');

-- Create a function to calculate party loyalty
CREATE OR REPLACE FUNCTION calculate_party_loyalty(mp_id_param INTEGER)
RETURNS NUMERIC AS $$
DECLARE
    total_votes INTEGER;
    party_line_votes INTEGER;
    loyalty_rate NUMERIC;
BEGIN
    -- This is a simplified version - would need more complex logic
    -- to determine what constitutes "party line" voting
    
    SELECT COUNT(*) INTO total_votes
    FROM vote_records vr
    JOIN voting_sessions vs ON vr.voting_session_id = vs.id
    WHERE vr.mp_id = mp_id_param AND vr.vote_result IN ('A', 'N', 'Z');
    
    -- For now, assume party loyalty is voting with majority of party members
    -- This would need more sophisticated calculation in real implementation
    
    IF total_votes = 0 THEN
        RETURN 0;
    END IF;
    
    loyalty_rate := 85.0; -- Placeholder calculation
    
    RETURN loyalty_rate;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE persons IS 'All persons in the system (MPs, senators, etc.)';
COMMENT ON TABLE mps IS 'Members of Parliament with their terms and contact info';
COMMENT ON TABLE vote_records IS 'Individual voting records for each MP on each vote';
COMMENT ON TABLE voting_sessions IS 'Parliamentary voting sessions with results';
COMMENT ON TABLE bills IS 'Parliamentary bills and legislative documents';
COMMENT ON TABLE committees IS 'Parliamentary committees and other organs';

COMMENT ON COLUMN vote_records.vote_result IS 'A=yes, N=no, Z=abstain, @=not registered, M=excused, X=did not vote, 0=not recorded';