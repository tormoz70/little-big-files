CREATE UNIQUE INDEX IF NOT EXISTS idx_package_files_member_sequence
    ON package_files(package_id, sequence_number)
    WHERE role = 'member';
