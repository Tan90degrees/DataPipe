# DataPipe 构建流程指南

## 概述

本文档详细说明了 DataPipe 项目的构建流程，包括 master、worker 和前端组件的构建、集成和部署策略。

## 项目架构

### 组件说明

- **Master**: 核心服务，负责 API 接口、任务调度和管理
- **Worker**: 工作节点，负责执行实际任务
- **UI**: Web 前端界面，与 Master 服务集成部署

## 构建流程

### 1. 本地开发构建

#### 前端构建

```bash
# 进入前端目录
cd ui/web

# 安装依赖
npm install

# 开发模式运行
npm run dev

# 生产构建
npm run build
```

#### Master 服务构建

```bash
# 项目根目录
go build -o datapipe-master ./cmd/master
```

#### Worker 服务构建

```bash
# 项目根目录
go build -o datapipe-worker ./cmd/worker
```

### 2. Docker 构建

#### Master 镜像构建（包含前端）

```bash
docker build -f deploy/docker/Dockerfile.master -t datapipe-master:latest .
```

#### Worker 镜像构建

```bash
docker build -f deploy/docker/Dockerfile.worker -t datapipe-worker:latest .
```

### 3. GitHub Actions 自动构建

#### 触发方式

1. **推送 Tag（v*）：自动触发发布构建
2. **手动触发**：通过 GitHub 界面的 "Run workflow" 按钮
3. **推送到 main 分支：触发 all 类型构建

#### 工作流步骤

1. **prepare**: 准备构建环境和变量
2. **build-go**: 构建 Go 二进制文件（master, worker, cli）
3. **build-ui**: 构建前端界面
4. **build-docker**: 构建并推送 Docker 镜像
5. **package-helm**: 打包 Helm 图表
6. **create-release**: 创建 GitHub Release

## 依赖关系

### 前端集成说明

- **前端** 与 **Master** 的集成方式：

1. Master 服务通过静态文件托管功能，将前端构建产物部署在配置的 UI 路径下
2. UI 路径可通过环境变量 `UI_PATH` 配置，默认为 `./ui`
3. 在 Docker 容器中，`UI_PATH` 被设置为 `/app/ui`
4. `/` 路由和所有非 API 路由都返回前端的 index.html
5. API 请求通过 `/api/v1` 路由处理

### 本地开发配置

在本地开发时，可以将前端构建产物链接或复制到项目根目录的 `./ui` 目录，或者通过环境变量指定路径：

```bash
# 方式1: 使用默认路径 ./ui
cp -r ui/web/dist/* ./ui/

# 方式2: 通过环境变量指定路径
UI_PATH=./ui/web/dist go run ./cmd/master
```

### Dockerfile 结构

**Dockerfile.master 采用多阶段构建：

1. **go-builder**: 构建 Master 二进制文件
2. **ui-builder**: 构建前端静态文件
3. **final**: 将两者整合到最终镜像中

## 错误排查方法

### 前端构建问题

1. **依赖安装失败**
```bash
cd ui/web
rm -rf node_modules package-lock.json
npm install
```

2. **TypeScript 类型错误**
```bash
npm run typecheck
```

### Master 服务静态文件问题

1. **检查 UI 路径**
确保前端构建产物路径是否正确：
```
/app/ui/index.html
/app/ui/static/
```

2. **健康检查**
访问 `/health` 端点验证服务是否正常运行

### GitHub Actions 问题

1. **create-release 未执行**
确保 `is_release` 变量设置为 `true`

2. **Docker 构建失败**
检查 Dockerfile 中的路径和文件是否正确

## 成功执行 create-release 的验证标准

1. **所有前置 Job 成功通过**
   - prepare
   - build-go
   - build-ui
   - build-docker
   - package-helm

2. **GitHub Release 成功创建**
   - 包含所有平台的二进制文件
   - 包含 UI 包
   - 包含 Helm 包
   - 包含 Docker 镜像信息

3. **Docker 镜像成功推送到 GHCR**
   - master 镜像
   - worker 镜像

## 部署策略

### 开发环境

使用 docker-compose 部署

```bash
cd deploy/docker
docker-compose up -d
```

### 生产环境

使用 Helm 部署到 Kubernetes

```bash
helm install datapipe deploy/kubernetes/helm/datapipe
```

## 版本发布流程

1. 创建 Tag
```bash
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

2. 等待 GitHub Actions 完成

3. 验证发布产物

## 总结

通过以上步骤，您可以完成 DataPipe 项目的完整构建流程，确保 master、worker 和前端组件能够协同工作并生成完整的发布版本。