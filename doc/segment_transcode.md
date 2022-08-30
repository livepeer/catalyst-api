
[![](https://mermaid.ink/img/pako:eNqlVMFu2zAM_RXC13WX9WYMAZI2w4K5nREnuyyFoUm0I8yWPEkO2hX991G2lThxgGFYDoFNvyc-PpJ6jbgWGMWRxV8tKo73kpWG1TsF9GuYcZLLhikH2e00Nk9X0-CDtO5KNE2A2e5jajTHRB6wQTQwhS48cGE0E5xZh6ZHUK73sxn9x7AxTFmv--MPM7NY1qiGjIw7eWAOT8oeNb1VWDjQBVxhO0qMYpzCS4xhLgRYZ5DVHuZMbg3Pn19-j07VB5Lfo3sknGCexCl2eXaaxJD5nITtZUhVevB3g0pIJ7WywLUqZPnUEwVOi6q0buCTNkDf9nDmAFCjjkWsVInWgVROn1cxCA4Ujw7qHtpnqBssnQ0He2phdA3Z1-36bnnJTROiLmJIW7sHrTpbpWpadylsEVKs0ZlW0WcykFUefyw-cGwgjZz21Husj_o88SAFau8l_2nPFZ05EMxGMWB7kc5Tc588f3fpCgX7h8GcbnaS1bdlvlnP777kySrb9KbKsgxDelJ7PmpD2j5rocEgR1oAca2dI60bTwpljaezG1toyPGLFma3MWybinYHLE1WhcBaIbvuM3WMda714k_G_WPB8GG6NGusqfj_3RuBFbqr3gySr9ozVp1us8_58vF-0p9Jh06D13auhbRHM_5yf3BdN4Pa6Caq0dRMCrpOXz17F7k91riLYnoUWLC2crtop94I2jaCVnpJybWJ4oJVFm8i1jqdvSgexbQgGEDDlTyg3v4A72LpfQ)](https://mermaid.live/edit#pako:eNqlVMFu2zAM_RXC13WX9WYMAZI2w4K5nREnuyyFoUm0I8yWPEkO2hX991G2lThxgGFYDoFNvyc-PpJ6jbgWGMWRxV8tKo73kpWG1TsF9GuYcZLLhikH2e00Nk9X0-CDtO5KNE2A2e5jajTHRB6wQTQwhS48cGE0E5xZh6ZHUK73sxn9x7AxTFmv--MPM7NY1qiGjIw7eWAOT8oeNb1VWDjQBVxhO0qMYpzCS4xhLgRYZ5DVHuZMbg3Pn19-j07VB5Lfo3sknGCexCl2eXaaxJD5nITtZUhVevB3g0pIJ7WywLUqZPnUEwVOi6q0buCTNkDf9nDmAFCjjkWsVInWgVROn1cxCA4Ujw7qHtpnqBssnQ0He2phdA3Z1-36bnnJTROiLmJIW7sHrTpbpWpadylsEVKs0ZlW0WcykFUefyw-cGwgjZz21Husj_o88SAFau8l_2nPFZ05EMxGMWB7kc5Tc588f3fpCgX7h8GcbnaS1bdlvlnP777kySrb9KbKsgxDelJ7PmpD2j5rocEgR1oAca2dI60bTwpljaezG1toyPGLFma3MWybinYHLE1WhcBaIbvuM3WMda714k_G_WPB8GG6NGusqfj_3RuBFbqr3gySr9ozVp1us8_58vF-0p9Jh06D13auhbRHM_5yf3BdN4Pa6Caq0dRMCrpOXz17F7k91riLYnoUWLC2crtop94I2jaCVnpJybWJ4oJVFm8i1jqdvSgexbQgGEDDlTyg3v4A72LpfQ)

# Future optimizations

- Duplicating `source` stream into `/dev/null` just to start `source` stream download from S3.
- Do not send audio track to Broadcaster if no audio processing takes place. This saves on network bandwidth moving audio to B-node and back.
- Instead several consecutive steps we can have single exposed API call to Mist server. Moving entire logic into Mist server.
- Calculate correctly completion percentage sent to Studio

# MistProcLivepeer

`MistProcLivepeer` is Mist server binary used for transcoding a stream using Livepeer network.

API go code starts it directly on the same machine Mist server is running. Usually `MistController` starts other `Mist*` binaries. The Mist server architecture allows this kind of integration.

Input is a `source` stream name and output is `sink` stream name.

`source` tracks example:
- AVC video track 1920x1080p
- AAC audio track 2c 4800Hz

`sink` tracks example:
- AVC video track 1280x720
- AVC video track 640x360
- AVC video track 160x180
- AAC audio track 2c 4800Hz

`source` stream is allowed to have multiple audio and video tracks, only best audio and best video track would be sent to Livepeer Broadcaster.

# LIVE_TRACK_LIST trigger

Mist server calls back to API server:
1) When first transcoded segment is ingested. We get info for each track:
  - Type: video/audio/text
  - Vido width
  - Video height
  - FPS
  - Codec
  - Starting timestamp
  - Latest timestamp
2) When last transcoded segment is processed. We get empty list of tracks.

This trigger lets us know we have first transcoded frames ready in `sink` stream. Then we start extracting separate mpegts files to store on S3 storage.

In our example above we have 3 transcoded video tracks in same `sink` stream on Mist server. We want to produce 3 separate mpegts segments containing each video track and same audio track.

Push mechanism from Mist server is used to produce those transcoded segments.

# PUSH_END trigger

This trigger is run whenever an outgoing push stops, either upload is complete or upload failed.

We get following info:
- Stream name
- Destination given in API call
- Actual destination used after expanding server variables. We don't use this feature.
- Push status - ok/failed

This is final stage of transcoding process.

One input segment is transcoded into multiple rendition segments. We wait for all uploads to complete to invoke Studio callback of entire operation.
1) Pull from S3 HTTP into SOURCE stream
2) Transcode from SOURCE to RENDITION
3) Push from RENDITION stream to S3 rendition 1
4) Push from RENDITION stream to S3 rendition 2
5) Push from RENDITION stream to S3 rendition 3
6) Invoke callback to Studio
