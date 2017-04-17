-- 初始化表结构
drop table if exists tickets;
drop table if exists replies;
drop table if exists config;

-- 工单表
create table tickets (
    ticket_id integer not null,
    category text not null,
    title text not null,
    content text,
    created_at integer,
    assignee text,
    status text,
    user text
);

-- 工单回复表
create table replies (
    reply_id integer not null,
    content text,
    user text,
    created_at integer,
    ticket_id integer not null
);

-- 配置表
create table config (
    key text not null,
    value text not null
);