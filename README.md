# USRP UDP 上位机发送与接收

这个项目包含两个上位机程序：

- `sender.py`：把图片或视频按固定分片封装成 UDP 数据帧并发出。
- `receiver.py`：接收 UDP 数据帧，按分片号重组文件；视频在收到连续 TS 数据后可实时播放。

中间 USRP 链路默认只做 UDP 透明转发；本项目不参与 LTE 侧纠错。

## 当前特性

- 图片传输后可按原文件恢复。
- 视频默认先转成 `MPEG-TS` 再发送，便于接收端边收边播。
- 接收端默认会在短暂等待后跳过丢失数据，继续往后直播播放。
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

如果 `.venv` 已经存在，脚本会直接复用它并更新依赖，不会重复重建。
如果你明确需要重建环境：

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -Recreate
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
python .\sender.py --input ".\input.jpg" --host 169.254.46.237 --port 50000
```

发送视频：

```powershell
python .\sender.py --input ".\input.mp4" --host 169.254.46.237 --port 50000
```

常用参数：

- `--chunk-size 1316`：每个 UDP 数据包负载大小，默认 `1316`，适合 MPEG-TS。
- `--target-rate-mbps 8`：按目标有效载荷吞吐率发包，推荐直接用这个参数做速率控制。
- `--packet-interval-us 200`：按固定包间隔发包；设置了 `--target-rate-mbps` 后会被忽略。
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
python .\receiver.py --bind-host 0.0.0.0 --port 60000 --output-dir .\received
```

如果只是做本机自动测试，不想弹播放窗口：

```powershell
python .\receiver.py --bind-host 127.0.0.1 --port 9000 --output-dir .\received --once --playback-no-display
```

常用参数：

- `--live-play` / `--no-live-play`：是否启用实时播放，默认开启。
- `--playback-buffer-kb 1024`：理想起播前的连续缓存大小；大一些更稳。
- `--playback-min-start-kb 256`：如果连续数据提前卡住，允许用这部分连续缓存提前起播。
- `--playback-start-timeout-ms 1000`：连续数据迟迟接不上时，等多久后按当前连续缓存提前起播。
- `--playback-gap-skip-ms 200`：如果直播被某个丢失 chunk 卡住这么久，就跳过这个缺口继续播；设为 `0` 表示严格等待，不跳过。
- `--playback-rewind-kb 4096`：关闭播放窗口后自动重启时可回放的最近缓存。
- `--idle-timeout 5`：超过多久没收到新数据就判定会话超时。
- `--auto-remux` / `--no-auto-remux`：接收完成后是否自动还原回原始容器。
- `--packet-queue-size 8192`：接收端用户态收包队列，抗突发更强。

接收端日志会显示：

- 当前接收进度
- 实时接收吞吐率 `rx`
- 实时送入连续播放缓冲的数据率 `feed`
- 当前缓存大小 `buffer`
- 已连续可播放到的分片位置 `ready`
- 如果出现 `wait N`，表示正在等第 `N` 个 chunk，后面的数据虽然收到了，但还不能继续播放
- 如果出现 `skipped N`，表示直播为了继续播放，已经跳过了 `N` 个丢失 chunk

## 8 MB/s 调参建议

如果你的目标是约 `8 MB/s` 有效载荷吞吐率，建议先这样测：

```powershell
python .\receiver.py --bind-host 0.0.0.0 --port 60000 --output-dir .\received --no-live-play --socket-buffer-kb 32768
python .\sender.py --input ".\input.mp4" --host 169.254.46.237 --port 50000 --target-rate-mbps 8 --chunk-size 1316
```

建议顺序：

- 先用 `--no-live-play` 测纯接收吞吐，确认网口和接收写盘能不能稳住 `8 MB/s`
- 纯接收稳定后，再打开实时播放看解码显示会不会拖慢
- 如果还不够，再尝试 `--chunk-size 1400`
- 如果只是做吞吐测试，可临时加 `--control-repeat 1 --control-interval-ms 0`，减少短文件测试里的控制包固定开销
- 如果接收端日志里长期停在 `wait 749` 这类位置，说明前面某个 chunk 没收到；默认直播模式会在短暂等待后跳过这个缺口继续播

说明：

- `8 MB/s` 有效载荷大约对应 `64 Mb/s` 以上的链路负载，算上 UDP/IP/以太网开销会更高一点
- 千兆网口基本没问题；百兆网口通常也还能试，但余量会小很多

## 典型流程

先启动接收端：

```powershell
python .\receiver.py --bind-host 0.0.0.0 --port 60000 --output-dir .\received --playback-buffer-kb 1024
```

再启动发送端：

```powershell
python .\sender.py --input ".\input.mp4" --host 169.254.46.237 --port 50000
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
