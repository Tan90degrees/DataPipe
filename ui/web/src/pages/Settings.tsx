import React, { useEffect, useState } from 'react';
import { Card, Form, Switch, Select, InputNumber, Button, message, Divider, Row, Col } from 'antd';
import { systemApi } from '../api/system';

interface SystemSettings {
  theme: 'light' | 'dark';
  language: string;
  timezone: string;
  notificationEnabled: boolean;
  autoRefresh: boolean;
  refreshInterval: number;
}

const Settings: React.FC = () => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadSettings();
  }, []);

  const loadSettings = async () => {
    try {
      setLoading(true);
      const data = await systemApi.getSettings();
      form.setFieldsValue(data);
    } catch {
      message.error('加载设置失败');
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    try {
      setSaving(true);
      const values = form.getFieldsValue();
      await systemApi.updateSettings(values);
      message.success('保存成功');
    } catch {
      message.error('保存失败');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div>
      <Card title="系统设置">
        <Form form={form} layout="vertical" loading={loading}>
          <Divider orientation="left">外观</Divider>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="theme" label="主题">
                <Select>
                  <Select.Option value="light">浅色</Select.Option>
                  <Select.Option value="dark">深色</Select.Option>
                </Select>
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="language" label="语言">
                <Select>
                  <Select.Option value="zh-CN">中文</Select.Option>
                  <Select.Option value="en-US">English</Select.Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>

          <Divider orientation="left">时间和区域</Divider>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="timezone" label="时区">
                <Select>
                  <Select.Option value="Asia/Shanghai">Asia/Shanghai (UTC+8)</Select.Option>
                  <Select.Option value="Asia/Tokyo">Asia/Tokyo (UTC+9)</Select.Option>
                  <Select.Option value="America/New_York">America/New_York (UTC-5)</Select.Option>
                  <Select.Option value="Europe/London">Europe/London (UTC+0)</Select.Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>

          <Divider orientation="left">通知和刷新</Divider>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="notificationEnabled" label="启用通知" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="autoRefresh" label="自动刷新" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="refreshInterval" label="刷新间隔 (秒)">
                <InputNumber min={5} max={300} />
              </Form.Item>
            </Col>
          </Row>

          <Divider />

          <Form.Item>
            <Button type="primary" onClick={handleSave} loading={saving}>
              保存设置
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </div>
  );
};

export default Settings;
