-- Создание таблицы videos (итоговая статистика по ролику)
CREATE TABLE IF NOT EXISTS videos (
    id BIGSERIAL PRIMARY KEY,
    creator_id BIGINT NOT NULL,
    video_created_at TIMESTAMP NOT NULL,
    views_count BIGINT NOT NULL DEFAULT 0,
    likes_count BIGINT NOT NULL DEFAULT 0,
    comments_count BIGINT NOT NULL DEFAULT 0,
    reports_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы video_snapshots (почасовые замеры по ролику)
CREATE TABLE IF NOT EXISTS video_snapshots (
    id BIGSERIAL PRIMARY KEY,
    video_id BIGINT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    views_count BIGINT NOT NULL DEFAULT 0,
    likes_count BIGINT NOT NULL DEFAULT 0,
    comments_count BIGINT NOT NULL DEFAULT 0,
    reports_count BIGINT NOT NULL DEFAULT 0,
    delta_views_count BIGINT NOT NULL DEFAULT 0,
    delta_likes_count BIGINT NOT NULL DEFAULT 0,
    delta_comments_count BIGINT NOT NULL DEFAULT 0,
    delta_reports_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS idx_videos_views_count ON videos(views_count);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_video_id ON video_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_created_at ON video_snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_video_created ON video_snapshots(video_id, created_at);

