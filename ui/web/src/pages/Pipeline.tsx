import React, { useEffect, useState } from 'react';
import { Table, Tag, Button, Space, Input, Modal, Form, Select, message, Popconfirm } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, PlayCircleOutlined, CopyOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { pipelineApi } from '../api/pipeline';
import type { Pipeline } from '../types/pipeline';
import DAGCanvas from '../components/DAGCanvas';

const PipelinePage: React.FC = () => {
  const navigate = useNavigate();
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [previewVisible, setPreviewVisible] = useState(false);
  const [selectedPipeline, setSelectedPipeline] = useState<Pipeline | null>(null);
  const [form] = Form.useForm();

  useEffect(() => {
    loadPipelines();
  }, []);

  const loadPipelines = async () => {
    try {
      setLoading(true);
      const response = await pipelineApi.list();
      setPipelines(response.pipelines);
    } catch (error) {
      message.error('加载管道列表失败');
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = () => {
    form.resetFields();
    setSelectedPipeline(null);
    setModalVisible(true);
  };

  const handleEdit = (record: Pipeline) => {
    navigate(`/pipeline/${record.id}/edit`);
  };

  const handleDelete = async (id: string) => {
    try {
      await pipelineApi.delete(id);
      message.success('删除成功');
      loadPipelines();
    } catch {
      message.error('删除失败');
    }
  };

  const handleExecute = async (id: string) => {
    try {
      await pipelineApi.execute(id);
      message.success('执行已启动');
      navigate('/monitor');
    } catch {
      message.error('启动执行失败');
    }
  };

  const handleDuplicate = async (id: string) => {
    try {
      await pipelineApi.duplicate(id);
      message.success('复制成功');
      loadPipelines();
    } catch {
      message.error('复制失败');
    }
  };

  const handlePreview = (record: Pipeline) => {
    setSelectedPipeline(record);
    setPreviewVisible(true);
  };

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      if (selectedPipeline) {
        await pipelineApi.update({ id: selectedPipeline.id, ...values });
        message.success('更新成功');
      } else {
        await pipelineApi.create(values);
        message.success('创建成功');
      }
      setModalVisible(false);
      loadPipelines();
    } catch {
      message.error('操作失败');
    }
  };

  const getStatusColor = (status: string) => {
    const colorMap: Record<string, string> = {
      active: 'green',
      draft: 'orange',
      paused: 'blue',
      error: 'red',
    };
    return colorMap[status] || 'default';
  };

  const columns = [
    { title: '名称', dataIndex: 'name', key: 'name' },
    { title: '描述', dataIndex: 'description', key: 'description', ellipsis: true },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => <Tag color={getStatusColor(status)}>{status}</Tag>,
    },
    { title: '节点数', key: 'nodeCount', render: (_: unknown, record: Pipeline) => record.nodes?.length || 0 },
    { title: '创建时间', dataIndex: 'createdAt', key: 'createdAt' },
    {
      title: '操作',
      key: 'action',
      render: (_: unknown, record: Pipeline) => (
        <Space size="small">
          <Button type="link" size="small" icon={<EditOutlined />} onClick={() => handleEdit(record)}>
            编辑
          </Button>
          <Button type="link" size="small" icon={<PlayCircleOutlined />} onClick={() => handleExecute(record.id)}>
            执行
          </Button>
          <Button type="link" size="small" onClick={() => handlePreview(record)}>
            预览
          </Button>
          <Button type="link" size="small" icon={<CopyOutlined />} onClick={() => handleDuplicate(record.id)}>
            复制
          </Button>
          <Popconfirm title="确认删除?" onConfirm={() => handleDelete(record.id)}>
            <Button type="link" size="small" danger icon={<DeleteOutlined />}>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Input.Search placeholder="搜索管道" style={{ width: 300 }} />
        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
          创建管道
        </Button>
      </div>

      <Table
        columns={columns}
        dataSource={pipelines}
        rowKey="id"
        loading={loading}
        pagination={{ pageSize: 10 }}
      />

      <Modal
        title={selectedPipeline ? '编辑管道' : '创建管道'}
        open={modalVisible}
        onOk={handleSubmit}
        onCancel={() => setModalVisible(false)}
        width={600}
      >
        <Form form={form} layout="vertical" initialValues={selectedPipeline || undefined}>
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入管道名称' }]}>
            <Input placeholder="请输入管道名称" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input.TextArea rows={3} placeholder="请输入管道描述" />
          </Form.Item>
          <Form.Item name="status" label="状态" initialValue="draft">
            <Select>
              <Select.Option value="draft">草稿</Select.Option>
              <Select.Option value="active">激活</Select.Option>
              <Select.Option value="paused">暂停</Select.Option>
            </Select>
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="管道预览"
        open={previewVisible}
        onCancel={() => setPreviewVisible(false)}
        footer={null}
        width={800}
      >
        {selectedPipeline && (
          <DAGCanvas
            nodes={selectedPipeline.nodes || []}
            edges={selectedPipeline.edges || []}
          />
        )}
      </Modal>
    </div>
  );
};

export default PipelinePage;
