# 项目简介/Project Introduction

ClkLog是一款记录并分析用户行为和画像的开源软件，技术人员可快速完成私有化部署。<br>
ClkLog is an open-source system that records and analyzes user online behaviors to build a user profile. Technical personnel can quickly complete private deployment.<br><br>

ClkLog基于神策分析SDK，采用ClickHouse数据库对采集数据进行存储，使用前后端分离的方式来实现。在这里，你可以轻松看到用户访问网页、APP、小程序或业务系统的行为轨迹，同时也可以从时间、地域、渠道、用户访客类型等多维度了解用户的全方位信息。<br>
ClkLog is based on the Sensors Analysis SDK. It uses the ClickHouse database to store collected data by using the front-end and back-end separation method. Here, you can easily see the users’ behavior track when they access the web pages, mobile apps, Wechat mini-programs or other business systems. You can also collect the users’ all-round information from multiple dimensions such as time, region, channel, visitor type, etc.<br><br>
ClkLog在开源社区版本的基础上同时提供拥有更多高级分析功能的商业版本。<br>
ClkLog also provides a commercial version with more advanced analysis functions based on the open-source community version.<br><br>

# 核心功能/Core Functions

- **数据采集**：支持网页、小程序、IOS、Android等多端数据采集<br>
- **Data collection**: supports data collection from multiple channels such as web pages, Wechat mini-programs, IOS, Android, etc.<br><br>

- **流量概览**：提供流量渠道、设备、地域、访客类型多维度分析<br>
- **Traffic overview**: provides multi-dimensional analysis from channels, devices, regions to visitor types.<br><br>
- **用户画像**：解析用户唯一ID，定位追踪用户全生命周期画像<br>
- **User Profile**: analyzes user unique IDs to locate and track full life cycle user profile.<br><br>
- **数据下载**：支持各项汇总数据、明细数据的下载<br>
- **Data Summary**: supports downloading of various summarized data and detailed data.

# 技术栈选择/Technology Selection

- **后端/Backend**：Redis 、Zookeeper、Kafka 、Flink

- **前端/Frontend**：vue、vue-element-admin、element-ui 、echarts

- **数据/Database**：Clickhouse、mysql

# 示意图/Screenshot Samples

| ![](https://clklog.com/assets/imgs/1.png) | ![](https://clklog.com/assets/imgs/2.png) |
| ----------------------------------------- | ----------------------------------------- |
| ![](https://clklog.com/assets/imgs/3.png) | ![](https://clklog.com/assets/imgs/4.png) |
| ![](https://clklog.com/assets/imgs/5.png) | ![](https://clklog.com/assets/imgs/6.png) |
| ![](https://clklog.com/assets/imgs/7.png) | ![](https://clklog.com/assets/imgs/8.png) |

# 在线体验/Online Demo

Demo address/演示地址：<a href="https://demo.clklog.com" target="_blank">https://demo.clklog.com</a>

# 快速接入/Quick Start Tutorial

官方文档/Official Documents<a href="https://clklog.com">https://clklog.com</a>

# 协议许可​/License Agreement​

## 开源协议/Open-source agreement：AGPLv3.0

使用的组织或个人在复制、分发、转发或修改时请遵守相关条款。任何分发或通过网络提供服务的版本（包括衍生版本）必须开源，并保留原版权和协议信息。如有违反，ClkLog将保留对侵权者追究责任的权利。<br>
Organizations or individuals using this software must comply with relevant terms when copying, distributing, ​​redistributing​​, or modifying it.Any distributed versions or versions provided as a network service (including derivative versions) must be open source with original copyright and license information preserved.ClkLog reserves the right to take legal action against infringers for any violations.<br><br>

## ​​免费使用 | Free Usage​​

**​​适用范围​​**：个人开发者、学术研究及非商业项目可免费使用<br>

**​​Scope​**​: Free for individual developers, academic research, and non-commercial projects.<br><br>

**​​商业限制**​​：若将ClkLog集成到闭源商业产品中，任何修改、二开、集成须遵循 AGPLv3.0 协议开源衍生产品<br>

**​​Commercial Restrictions​**​: If integrated into closed-source commercial products, ​​any modification, secondary development, or integration shall open-source derivative works​​ under AGPLv3.0.<br><br>

**​​授权方式​**​：遵循 AGPLv3.0 协议<br>

**​​Licensing Mode**​​: Subject to AGPLv3.0 license.<br><br>

## ​​商业授权 | Commercial License​​

**​​适用范围**​​：商业项目集成可闭源使用<br>

**​​Scope**​​: Permits closed-source integration for commercial projects.<br><br>

**​​授权方式​**​：需购买商业授权<br>

**​​Licensing Mode​​**: Requires purchasing a commercial license.<br><br>

## ​​特别提醒 | Special Notice

​​在AGPL V3.0协议中​​，“衍生产品”是指：在 ClkLog 源代码基础上进行任何修改、扩展、适配、重构，或与其他软件、系统组合后形成的作品，包括但不限于：<br>

​​Under AGPLv3.0​​, ​​"Derivative Works"​​ refer to any works created through modification, extension, adaptation, refactoring of ClkLog source code, or combination with other software/systems, including but not limited to:<br>

• 修改、删除或新增源代码的版本；<br>

• Versions with modified, deleted, or added source code;<br>

• 增加功能模块、插件或集成接口的版本；<br>

• Versions adding functional modules, plugins, or integration interfaces;<br>

• 将 ClkLog 嵌入或整合进其他产品、系统或服务的版本；<br>

• Versions embedding or integrating ClkLog into other products, systems, or services;<br>

• 改变数据结构、接口协议或运行架构的版本。<br>

• Versions altering data structures, interface protocols, or runtime architectures.<br>

无论改动大小，只要衍生产品包含 ClkLog 的代码或核心逻辑，即视为衍生产品，并适用本协议的相关条款。<br>

​​Regardless of modification scale, any work containing ClkLog's code or core logic shall constitute a Derivative Work and is subject to relevant terms of AGPLv3.0.​<br>

# 联系我们/Contact Us

- 客服手机/Customer service mobile：16621363853

- 客服微信/Customer service WeChat：opensoft66

- 客服二维码/Customer service QR Code:<img title="" src="https://clklog.com/assets/imgs/contactqrcode.jpg" alt="" data-align="center" width="120" style="vertical-align:top">
