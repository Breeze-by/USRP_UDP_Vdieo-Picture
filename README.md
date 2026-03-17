# USRP UDP 上位机发送与接收

这个项目包含两个上位机程序：

- `sender.py`：把图片或视频按固定分片封装成 UDP 数据帧并发出。
- `receiver.py`：接收 UDP 数据帧，按分片号重组文件；视频在收到连续 TS 数据后可实时播放。

中间 USRP 链路默认只做 UDP 透明转发；本项目不参与 LTE 侧纠错。

## 当前特性

- 图片传输后可按原文件恢复。
- 视频默认先转成 `MPEG-TS` 再发送，便于接收端边收边播。
- 接收端只按连续正确到达的数据播放。
- 如果传输变慢或中间缺数据，播放会卡住等待，不会主动补帧或跳帧。
- 如果传输更快，接收端会积累缓存再按视频时间戳播放。
- 如果手动关闭播放窗口，接收端会基于最近缓存自动重启播放器。

## 环境安装

现在不再需要系统单独安装 `ffmpeg` 或 `ffplay`。

`requirements.txt` 已经包含运行所需 Python 依赖，下面这条命令可以直接装全：

```powershell
pip install -r .\requirements.txt
```

更推荐用项目自己的虚拟环境：

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1
```

或者手工执行：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r .\requirements.txt
```

安装完成后，视频相关能力由这些包提供：

- `imageio-ffmpeg`：发送端视频转 TS，接收端收完后的容器还原
- `av`：接收端实时解码 MPEG-TS
- `opencv-python`：接收端实时显示视频窗口

说明：

- 实时播放是视频播放，不额外输出实时音频。
- 收完后还原出的原始文件仍会保留音视频内容。

## 发送端用法

发送图片：

```powershell
python .\sender.py --input ".\input.jpg" --host 192.168.10.2 --port 6000
```

发送视频：

```powershell
python .\sender.py --input ".\input.mp4" --host 192.168.10.2 --port 6000
```

常用参数：

- `--chunk-size 1316`：每个 UDP 数据包负载大小，默认 `1316`，适合 MPEG-TS。
- `--packet-interval-us 200`：发包间隔，链路容易拥塞时可适当调大。
- `--streamable-ts` / `--no-streamable-ts`：视频是否先转成 TS 再发送。

发送端日志会显示：

- 输入文件与实际传输文件
- 是否转成了流式 TS
- 总分片数
- 当前发送吞吐率
- 当前包速率和预计剩余时间

## 接收端用法

接收并实时播放视频：

```powershell
python .\receiver.py --bind-host 0.0.0.0 --port 6000 --output-dir .\received
```

如果只是做本机自动测试，不想弹播放窗口：

```powershell
python .\receiver.py --bind-host 127.0.0.1 --port 9000 --output-dir .\received --once --playback-no-display
```

常用参数：

- `--live-play` / `--no-live-play`：是否启用实时播放，默认开启。
- `--playback-buffer-kb 2048`：起播前缓存大小；小一些起播更快，大一些更稳。
- `--playback-rewind-kb 4096`：关闭播放窗口后自动重启时可回放的最近缓存。
- `--idle-timeout 5`：超过多久没收到新数据就判定会话超时。
- `--auto-remux` / `--no-auto-remux`：接收完成后是否自动还原回原始容器。

接收端日志会显示：

- 当前接收进度
- 实时接收吞吐率 `rx`
- 实时送入播放器的数据率 `play`
- 当前缓存大小 `buffer`
- 已连续可播放到的分片位置 `ready`

## 典型流程

先启动接收端：

```powershell
python .\receiver.py --bind-host 0.0.0.0 --port 6000 --output-dir .\received --playback-buffer-kb 2048
```

再启动发送端：

```powershell
python .\sender.py --input ".\input.mp4" --host 192.168.10.2 --port 6000
```

如果是本机回环测试，把发送端目标地址改成 `127.0.0.1`，端口与接收端保持一致。

## 输出目录

每次接收结果默认保存在：

```text
received/session_<session_id>/
```

通常包含：

- 传输过程中的 `transport` 文件
- 自动还原出的原始文件
- `manifest.json`

## 与 USRP 对接时的建议

- 发送端 `--host` / `--port` 指向发射侧 USRP 的 UDP 输入地址。
- 接收端监听端口与接收侧 USRP 的 UDP 输出端口保持一致。
- 如果链路吞吐不够，实时播放会卡住等待后续连续数据，这是当前设计预期。
- 如果链路吞吐有余量，接收端缓存会变大，播放会更稳。
- 如果中间链路 MTU 偏小，减小 `--chunk-size`，避免底层 IP 分片。
