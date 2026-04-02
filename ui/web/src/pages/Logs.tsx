import React, { useEffect, useState } from 'react';
import { Card, Table, Input, Select, DatePicker, Button, Space, Tag, Row, Col, Spin, message } from 'antd';
import { SearchOutlined, ReloadOutlined, ExportOutlined, DownloadOutlined } from '@ant-design/icons';
import { executionApi } from '../api/execution';
import type { ExecutionLog } from '../types/execution';
import type { Dayjs } from 'dayjs';

const { RangePicker } = DatePicker;

const Logs: React.FC = () => {
  const [logs, setLogs] = useState<ExecutionLog[]>([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [keyword, setKeyword] = useState('');
  const [levelFilter, setLevelFilter] = useState<string>('all');
  const [executionId, setExecutionId] = useState<string | undefined>();

  useEffect(() => {
    loadLogs();
  }, [page, pageSize, levelFilter]);

  const loadLogs = async () => {
    try {
      setLoading(true);
      const response = await executionApi.searchLogs({
        page,
        pageSize,
        level: levelFilter !== 'all' ? levelFilter : undefined,
        keyword: keyword || undefined,
        executionId,
      });
      setLogs(response.logs);
      setTotal(response.total);
    } catch (error) {
      console.error('Failed to load logs:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = () => {
    setPage(1);
    loadLogs();
  };

  const handleExport = async () => {
    try {
      const blob = await executionApi.exportLogs({
        executionId,
        format: 'csv',
      });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `logs_${Date.now()}.csv`;
      a.click();
      window.URL.revokeObjectURL(url);
      message.success('导出成功');
    } catch {
      message.error('导出失败');
    }
  };

  const getLevelColor = (level: string) => {
    const colorMap: Record<string, string> = {
      info: 'blue',
      warning: 'orange',
      error: 'red',
    };
    return colorMap[level] || 'default';
  };

  const columns = [
    {
      title: '时间',
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: 180,
    },
    {
      title: '级别',
      dataIndex: 'level',
      key: 'level',
      width: 80,
      render: (level: string) => <Tag color={getLevelColor(level)}>{level.toUpperCase()}</Tag>,
    },
    { title: '节点', dataIndex: 'nodeName', key: 'nodeName', width: 150 },
    { title: '消息', dataIndex: 'message', key: 'message', ellipsis: true },
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={16} align="middle">
          <Col span={6}>
            <Input
              placeholder="搜索关键字"
              prefix={<SearchOutlined />}
              value={keyword}
              onChange={(e) => setKeyword(e.target.value)}
              onPressEnter={handleSearch}
            />
          </Col>
          <Col span={4}>
            <Select
              value={levelFilter}
              onChange={(value) => { setLevelFilter(value); setPage(1); }}
              style={{ width: '100%' }}
            >
              <Select.Option value="all">全部级别</Select.Option>
              <Select.Option value="info">INFO</Select.Option>
              <Select.Option value="warning">WARNING</Select.Option>
              <Select.Option value="error">ERROR</Select.Option>
            </Select>
          </Col>
          <Col span={4}>
            <Input
              placeholder="执行ID"
              value={executionId}
              onChange={(e) => setExecutionId(e.target.value || undefined)}
            />
          </Col>
          <Col span={10}>
            <Space>
              <Button type="primary" icon={<SearchOutlined />} onClick={handleSearch}>
                搜索
              </Button>
              <Button icon={<ReloadOutlined />} onClick={loadLogs}>
                刷新
              </Button>
              <Button icon={<DownloadOutlined />} onClick={handleExport}>
                导出
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      <Card title={`日志列表 (共 ${total} 条)`}>
        <Table
          columns={columns}
          dataSource={logs}
          rowKey="id"
          loading={loading}
          pagination={{
            current: page,
            pageSize,
            total,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (t) => `共 ${t} 条`,
            onChange: (p, ps) => {
              setPage(p);
              setPageSize(ps);
            },
          }}
          size="small"
        />
      </Card>
    </div>
  );
};

export default Logs;
