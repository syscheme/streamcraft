# MJPEG输出部分要求1:10浅压缩且CBR

要使用 ffmpeg 将高清视频转码为浅压缩的 MJPEG 视频流，并且保持 1:10 的浅压缩比以及恒定比特率 (CBR)，需要对 ffmpeg 的命令做一些调整。由于 MJPEG 编码器通常不支持直接设置压缩比，可以间接通过调整比特率和质量来实现类似的效果。

这里的目标是：

- 保持 1:10 的浅压缩比。
- 设置恒定比特率 (CBR)。
- 对于 1:10 的浅压缩比，假设原始视频的比特率为 100 Mbps（这是一个非常高的比特率），那么浅压缩后的目标比特率大约为 10 Mbps。请注意，这只是一个示例，实际的原始比特率和目标比特率取决于视频的内容和分辨率。

以下是一个示例命令，用于将高清视频转码为浅压缩的 MJPEG 视频流，同时保持 CBR 和 1:10 的浅压缩比：

```
ffmpeg -i input.mp4 \
       -c:v mjpeg \
       -b:v 10M \
       -maxrate 10M \
       -bufsize 20M \
       -q:v 1 \
       -c:a pcm_s16le \
       output.mjpeg
```
逐个解析这些选项：
  -  -i input.mp4: 输入文件名。
  -  -c:v mjpeg: 使用 MJPEG 编码器。
  -  -b:v 10M: 设置视频比特率为 10 Mbps。这是根据 1:10 的浅压缩比来设定的。
  -  -maxrate 10M: 设置最大比特率为 10 Mbps。这对于 CBR 是可选的，但有助于确保不会超出比特率限制。
  -  -bufsize 20M: 设置缓冲区大小为 20 Mbps。这有助于避免缓冲区溢出，尤其是在视频内容变化较大的情况下。
  -  -q:v 1: 设置视频质量等级为 1（数值越小质量越高）。这相当于设置 JPEG 的压缩比。可以尝试不同的值来达到期望的压缩比。
  -  -c:a pcm_s16le: 设置音频编码器为 16-bit little-endian PCM。
  -  output.mjpeg: 输出文件名。

注意事项
- MJPEG 编码器通常不直接支持设置压缩比，因此通过调整比特率和质量等级来间接实现。
- 1:10 的压缩比是一个示例值，需要根据实际情况调整比特率。
- 可能需要尝试不同的 -q:v 值来达到接近 1:10 的压缩比。通常，更高的质量等级会导致更低的压缩比，反之亦然。
- 如果发现输出的压缩比不符合预期，可以尝试增加或减少比特率和调整质量等级。
- 如果想要更精确地控制压缩比，可能需要使用其他编码器或额外的工具。


----

# Padding到相同的字节大小
使用 MJPEG 编码器时，每帧的 JPEG 图像可能会有不同的字节大小，这取决于图像的内容和复杂程度。即使设置了恒定比特率 (CBR)，由于 MJPEG 编码器通常不会强制每帧的大小保持一致，因此每帧的大小可能会有所不同。

如果希望每帧的 JPEG 图像具有相同的字节大小，需要采取额外的措施来实现这一目标。一种方法是在每帧后面添加填充数据，使每一帧的大小达到一个固定的值。这里是一个简化的示例，展示如何使用 ffmpeg 和 shell 脚本来实现这一目标：

使用 ffmpeg 生成 MJPEG 视频流: 首先，使用 ffmpeg 生成 MJPEG 视频流，并设置比特率和质量等级。
```
ffmpeg -i input.mp4 \
       -c:v mjpeg \
       -b:v 10M \
       -maxrate 10M \
       -bufsize 20M \
       -q:v 1 \
       -c:a pcm_s16le \
       -f mjpeg \
       -y \
       - |
```
使用 shell 脚本进行填充: 可以使用 shell 脚本来读取 ffmpeg 的输出并添加填充数据。这里是一个简化的脚本示例：
```
#!/bin/bash

# 定义每帧的目标大小
TARGET_FRAME_SIZE=10240 # 例如，每帧 10 KB

# 从 ffmpeg 获取 MJPEG 视频流
ffmpeg -i input.mp4 \
       -c:v mjpeg \
       -b:v 10M \
       -maxrate 10M \
       -bufsize 20M \
       -q:v 1 \
       -c:a pcm_s16le \
       -f mjpeg \
       -y \
       - | while read -d '' -n "$TARGET_FRAME_SIZE" frame; do
   # 获取当前帧的实际大小
   FRAME_SIZE=${#frame}

   # 计算需要填充的字节数
   PADDING_SIZE=$((TARGET_FRAME_SIZE - FRAME_SIZE))

   # 添加填充数据
   frame+=$(printf '\0%.0s' $(seq 1 $PADDING_SIZE))

   # 将填充后的帧写入文件
   echo -n "$frame" >> output.mjpeg
done
```
解释
- TARGET_FRAME_SIZE 定义了每帧的目标大小。
- while read -d '' -n "$TARGET_FRAME_SIZE" frame; do 从 ffmpeg 的输出中读取每一帧。
- FRAME_SIZE=${#frame} 计算当前帧的实际大小。
- PADDING_SIZE=$((TARGET_FRAME_SIZE - FRAME_SIZE)) 计算需要填充的字节数。
- frame+=$(printf '\0%.0s' $(seq 1 $PADDING_SIZE)) 添加填充数据。
- echo -n "$frame" >> output.mjpeg 将填充后的帧追加到输出文件中。
注意事项
- 这个脚本假设每帧的大小不超过 TARGET_FRAME_SIZE。
- 如果帧的实际大小超过了 TARGET_FRAME_SIZE，脚本将无法正确处理这种情况。
- 可能需要根据实际的视频内容和压缩比来调整 TARGET_FRAME_SIZE 的值。
- 这个脚本使用零字节作为填充数据。可以选择其他字符或模式作为填充。
运行脚本
- 将脚本保存为文件，例如 pad_mjpeg.sh。
- 给脚本添加执行权限：chmod +x pad_mjpeg.sh.
- 运行脚本：./pad_mjpeg.sh.
这种方法可以确保输出文件中的每帧具有相同的字节大小，但需要注意的是，这种方法可能会引入额外的延迟，因为脚本需要等待每一帧达到目标大小后才能继续处理。此外，如果某些帧的实际大小超过了目标大小，脚本将无法正确处理这种情况，因此可能需要根据实际情况调整脚本以适应不同的场景。


MJPEG（Motion JPEG）是一种视频编码标准，它通过将一系列JPEG图像序列化来构成视频流。每个JPEG图像都独立编码，并且通常会在每个JPEG帧的开始处包含一些必要的头信息，这些信息对于解码该帧至关重要。因此，在对MJPEG视频流进行填充时，需要确保不破坏这些头信息。

----
## MJPEG 帧结构
MJPEG 视频流中的每一帧都是一个完整的 JPEG 图像。一个典型的 JPEG 文件的结构如下：

- SOI (Start of Image): 标志着 JPEG 图像的开始，值为 FF D8。
- App Segments: 可能包括一些应用特定的数据段，比如 Exif 数据等。
- DQT (Define Quantization Table): 定义量化表。
- DHT (Define Huffman Table): 定义霍夫曼表。
- SOF (Start of Frame): 开始帧标志，定义了图像的尺寸和其他相关信息。
- SOS (Start of Scan): 开始扫描标志，标志着图像数据的开始。
- EOD (End of Data): 扫描数据结束标志，通常与 SOS 相对应。
- EOI (End of Image): 标志着 JPEG 图像的结束，值为 FF D9。

----
## 在尾部进行 Padding
为了不影响 JPEG 图像的解码，需要在 EOI 标志之后进行填充。这意味着在 JPEG 图像的末尾添加额外的字节，这些字节不会影响解码过程，因为解码器会寻找下一个 SOI 标志来确定新的图像帧的开始。

实现步骤
- 获取每帧的大小:
  - 使用 ffmpeg 或其他工具提取每帧的大小。
- 计算 Padding 大小:
- 根据目标帧大小计算出每帧需要填充多少字节。
- 添加 Padding:
- 在每帧的 EOI 标志之后添加所需的字节数。

示例脚本
下面是一个简单的 Bash 脚本示例，用于读取 MJPEG 流并进行填充：

```
#!/bin/bash

# 定义每帧的目标大小
TARGET_FRAME_SIZE=10240 # 例如，每帧 10 KB

# 从 ffmpeg 获取 MJPEG 视频流
ffmpeg -i input.mp4 \
       -c:v mjpeg \
       -b:v 10M \
       -maxrate 10M \
       -bufsize 20M \
       -q:v 1 \
       -c:a pcm_s16le \
       -f mjpeg \
       -y \
       - | while IFS= read -d '' -r -n 1 byte; do
         # 逐字节读取，直到找到 EOI (FF D9)
         if [[ $byte == $'\xFF' ]]; then
           # 下一个字节可能是 D9
           read -d '' -r -n 1 next_byte
           if [[ $next_byte == $'\xD9' ]]; then
             # 找到了 EOI 标志
             echo -n $byte$next_byte >> frame.dat
             # 计算当前帧的实际大小
             FRAME_SIZE=$(stat -c %s frame.dat)

             # 计算需要填充的字节数
             PADDING_SIZE=$((TARGET_FRAME_SIZE - FRAME_SIZE))

             # 清空当前帧文件
             > frame.dat

             # 添加填充数据
             printf '\0%.0s' $(seq 1 $PADDING_SIZE) >> frame.dat

             # 将填充后的帧写入文件
             cat frame.dat >> output.mjpeg
           else
             # 没有找到 D9，继续读取
             echo -n $byte$next_byte >> frame.dat
           fi
         else
           # 写入当前字节
           echo -n $byte >> frame.dat
         fi
       done
```
注意事项
- 这个脚本假设每帧的大小不超过 TARGET_FRAME_SIZE。
- 如果帧的实际大小超过了 TARGET_FRAME_SIZE，脚本将无法正确处理这种情况。
- 这个脚本使用零字节作为填充数据。可以选择其他字符或模式作为填充。
- 该脚本可能需要根据实际情况进行调整，特别是在处理大型文件或不同类型的输入时。

运行脚本
- 将脚本保存为文件，例如 pad_mjpeg.sh。
- 给脚本添加执行权限：chmod +x pad_mjpeg.sh.
- 运行脚本：./pad_mjpeg.sh.

这个脚本将确保在每个 JPEG 图像的末尾添加足够的填充数据，同时保持图像的完整性，从而使得解码器能够正确地解析出每一帧。