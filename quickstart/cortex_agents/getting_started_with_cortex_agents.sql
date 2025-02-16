-- 2 ワークスペースの設定
-- 2 ワークスペースの設定
-- create database and schema
use role sysadmin;
create database if not exists d_harato_db;
create schema if not exists d_harato_db.data;
create warehouse if not exists d_harato_wh;

-- create tables for sales data
create or replace table sales_conversations (
    conversation_id varchar,
    transcript_text text,
    customer_name varchar,
    deal_stage varchar,
    sales_rep varchar,
    conversation_date timestamp,
    deal_value float,
    product_line varchar
);

create or replace table sales_metrics (
    deal_id varchar,
    customer_name varchar,
    deal_value float,
    close_date date,
    sales_stage varchar,
    win_status boolean,
    sales_rep varchar,
    product_line varchar
);

-- first, let's insert data into sales_conversations
insert into sales_conversations 
(conversation_id, transcript_text, customer_name, deal_stage, sales_rep, conversation_date, deal_value, product_line)
values
('conv001', 'initial discovery call with techcorp inc''s it director and solutions architect. client showed strong interest in our enterprise solution features, particularly the automated workflow capabilities. main discussion centered around integration timeline and complexity. they currently use legacy system x for their core operations and expressed concerns about potential disruption during migration. team asked detailed questions about api compatibility and data migration tools. action items: 1) provide detailed integration timeline document 2) schedule technical deep-dive with their infrastructure team 3) share case studies of similar legacy system x migrations. client mentioned q2 budget allocation for digital transformation initiatives. overall positive engagement with clear next steps.', 'techcorp inc', 'discovery', 'sarah johnson', '2024-01-15 10:30:00', 75000, 'enterprise suite'),

('conv002', 'follow-up call with smallbiz solutions'' operations manager and finance director. primary focus was on pricing structure and roi timeline. they compared our basic package pricing with competitor y''s small business offering. key discussion points included: monthly vs. annual billing options, user license limitations, and potential cost savings from process automation. client requested detailed roi analysis focusing on: 1) time saved in daily operations 2) resource allocation improvements 3) projected efficiency gains. budget constraints were clearly communicated - they have a maximum budget of $30k for this year. showed interest in starting with basic package with room for potential upgrade in q4. need to provide competitive analysis and customized roi calculator by next week.', 'smallbiz solutions', 'negotiation', 'mike chen', '2024-01-16 14:45:00', 25000, 'basic package'),

('conv003', 'strategy session with securebank ltd''s ciso and security operations team. extremely positive 90-minute deep dive into our premium security package. customer emphasized immediate need for implementation due to recent industry compliance updates. our advanced security features, especially multi-factor authentication and encryption protocols, were identified as perfect fits for their requirements. technical team was particularly impressed with our zero-trust architecture approach and real-time threat monitoring capabilities. they''ve already secured budget approval and have executive buy-in. compliance documentation is ready for review. action items include: finalizing implementation timeline, scheduling security audit, and preparing necessary documentation for their risk assessment team. client ready to move forward with contract discussions.', 'securebank ltd', 'closing', 'rachel torres', '2024-01-17 11:20:00', 150000, 'premium security'),

('conv004', 'comprehensive discovery call with growthstart up''s cto and department heads. team of 500+ employees across 3 continents discussed current challenges with their existing solution. major pain points identified: system crashes during peak usage, limited cross-department reporting capabilities, and poor scalability for remote teams. deep dive into their current workflow revealed bottlenecks in data sharing and collaboration. technical requirements gathered for each department. platform demo focused on scalability features and global team management capabilities. client particularly interested in our api ecosystem and custom reporting engine. next steps: schedule department-specific workflow analysis and prepare detailed platform migration plan.', 'growthstart up', 'discovery', 'sarah johnson', '2024-01-18 09:15:00', 100000, 'enterprise suite'),

('conv005', 'in-depth demo session with datadriven co''s analytics team and business intelligence managers. showcase focused on advanced analytics capabilities, custom dashboard creation, and real-time data processing features. team was particularly impressed with our machine learning integration and predictive analytics models. competitor comparison requested specifically against market leader z and innovative start-up x. price point falls within their allocated budget range, but team expressed interest in multi-year commitment with corresponding discount structure. technical questions centered around data warehouse integration and custom visualization capabilities. action items: prepare detailed competitor feature comparison matrix and draft multi-year pricing proposals with various discount scenarios.', 'datadriven co', 'demo', 'james wilson', '2024-01-19 13:30:00', 85000, 'analytics pro'),

('conv006', 'extended technical deep dive with healthtech solutions'' it security team, compliance officer, and system architects. four-hour session focused on api infrastructure, data security protocols, and compliance requirements. team raised specific concerns about hipaa compliance, data encryption standards, and api rate limiting. detailed discussion of our security architecture, including: end-to-end encryption, audit logging, and disaster recovery protocols. client requires extensive documentation on compliance certifications, particularly soc 2 and hitrust. security team performed initial architecture review and requested additional information about: database segregation, backup procedures, and incident response protocols. follow-up session scheduled with their compliance team next week.', 'healthtech solutions', 'technical review', 'rachel torres', '2024-01-20 15:45:00', 120000, 'premium security'),

('conv007', 'contract review meeting with legalease corp''s general counsel, procurement director, and it manager. detailed analysis of sla terms, focusing on uptime guarantees and support response times. legal team requested specific modifications to liability clauses and data handling agreements. procurement raised questions about payment terms and service credit structure. key discussion points included: disaster recovery commitments, data retention policies, and exit clause specifications. it manager confirmed technical requirements are met pending final security assessment. agreement reached on most terms, with only sla modifications remaining for discussion. legal team to provide revised contract language by end of week. overall positive session with clear path to closing.', 'legalease corp', 'negotiation', 'mike chen', '2024-01-21 10:00:00', 95000, 'enterprise suite'),

('conv008', 'quarterly business review with globaltrade inc''s current implementation team and potential expansion stakeholders. current implementation in finance department showcasing strong adoption rates and 40% improvement in processing times. discussion focused on expanding solution to operations and hr departments. users highlighted positive experiences with customer support and platform stability. challenges identified in current usage: need for additional custom reports and increased automation in workflow processes. expansion requirements gathered from operations director: inventory management integration, supplier portal access, and enhanced tracking capabilities. hr team interested in recruitment and onboarding workflow automation. next steps: prepare department-specific implementation plans and roi analysis for expansion.', 'globaltrade inc', 'expansion', 'james wilson', '2024-01-22 14:20:00', 45000, 'basic package'),

('conv009', 'emergency planning session with fasttrack ltd''s executive team and project managers. critical need for rapid implementation due to current system failure. team willing to pay premium for expedited deployment and dedicated support team. detailed discussion of accelerated implementation timeline and resource requirements. key requirements: minimal disruption to operations, phased data migration, and emergency support protocols. technical team confident in meeting aggressive timeline with additional resources. executive sponsor emphasized importance of going live within 30 days. immediate next steps: finalize expedited implementation plan, assign dedicated support team, and begin emergency onboarding procedures. team to reconvene daily for progress updates.', 'fasttrack ltd', 'closing', 'sarah johnson', '2024-01-23 16:30:00', 180000, 'premium security'),

('conv010', 'quarterly strategic review with upgradenow corp''s department heads and analytics team. current implementation meeting basic needs but team requiring more sophisticated analytics capabilities. deep dive into current usage patterns revealed opportunities for workflow optimization and advanced reporting needs. users expressed strong satisfaction with platform stability and basic features, but requiring enhanced data visualization and predictive analytics capabilities. analytics team presented specific requirements: custom dashboard creation, advanced data modeling tools, and integrated bi features. discussion about upgrade path from current package to analytics pro tier. roi analysis presented showing potential 60% improvement in reporting efficiency. team to present upgrade proposal to executive committee next month.', 'upgradenow corp', 'expansion', 'rachel torres', '2024-01-24 11:45:00', 65000, 'analytics pro');

-- now, let's insert corresponding data into sales_metrics
insert into sales_metrics 
(deal_id, customer_name, deal_value, close_date, sales_stage, win_status, sales_rep, product_line)
values
('deal001', 'techcorp inc', 75000, '2024-02-15', 'closed', true, 'sarah johnson', 'enterprise suite'),

('deal002', 'smallbiz solutions', 25000, '2024-02-01', 'lost', false, 'mike chen', 'basic package'),

('deal003', 'securebank ltd', 150000, '2024-01-30', 'closed', true, 'rachel torres', 'premium security'),

('deal004', 'growthstart up', 100000, '2024-02-10', 'pending', false, 'sarah johnson', 'enterprise suite'),

('deal005', 'datadriven co', 85000, '2024-02-05', 'closed', true, 'james wilson', 'analytics pro'),

('deal006', 'healthtech solutions', 120000, '2024-02-20', 'pending', false, 'rachel torres', 'premium security'),

('deal007', 'legalease corp', 95000, '2024-01-25', 'closed', true, 'mike chen', 'enterprise suite'),

('deal008', 'globaltrade inc', 45000, '2024-02-08', 'closed', true, 'james wilson', 'basic package'),

('deal009', 'fasttrack ltd', 180000, '2024-02-12', 'closed', true, 'sarah johnson', 'premium security'),

('deal010', 'upgradenow corp', 65000, '2024-02-18', 'pending', false, 'rachel torres', 'analytics pro');

-- enable change tracking
alter table sales_conversations set change_tracking = true;


-- Create the search service
create or replace cortex search service sales_conversation_search
  on transcript_text
  attributes customer_name, deal_stage, sales_rep
  warehouse = sales_intelligence_wh
  target_lag = '1 hour'
  as (
    select
        transcript_text,
        customer_name,
        deal_stage,
        sales_rep,
        conversation_date
    from sales_conversations
    where conversation_date >= '2024-01-01' 
);

create or replace stage models directory = (enable = true);