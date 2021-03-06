/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
<template>
  <m-popup
          ref="popup"
          :ok-text="$t('Upload')"
          :nameText="$t('File Upload')"
          @ok="_ok"
          :disabled="progress === 0 ? false : true">
    <template slot="content">
      <form name="files" enctype="multipart/form-data" method="post">
        <div class="file-update-model"
             @drop.prevent="_onDrop"
             @dragover.prevent="dragOver = true"
             @dragleave.prevent="dragOver = false"
             id="file-update-model">
          <div class="tooltip-info">
            <i class="fa fa-info-circle"></i>
            <span>{{$t('Drag the file into the current upload window')}}</span>
          </div>
          <!--<div class="hide-archive" v-if="progress !== 0" @click="_ckArchive">
            <i class="fa fa-minus" data-toggle="tooltip" title="关闭窗口 继续上传" data-container="body" ></i>
          </div>-->
          <div class="update-popup" v-if="dragOver">
            <div class="icon-box">
              <i class="fa fa-cloud-upload"></i>
            </div>
            <p class="p1">
              <span>{{$t('Drag area upload')}}</span>
            </p>
          </div>
          <m-list-box-f>
            <template slot="name"><b>*</b>{{$t('Upload Files')}}</template>
            <template slot="content">
              <div class="file-update-box">
                <template v-if="progress === 0">
                  <input name="file" id="file" type="file" class="file-update">
                  <x-button type="dashed" size="xsmall"> {{$t('Upload')}} </x-button>
                </template>
                <div class="progress-box" v-if="progress !== 0">
                  <m-progress-bar :value="progress" text-placement="left-right"></m-progress-bar>
                </div>
              </div>
            </template>
          </m-list-box-f>
          <m-list-box-f>
            <template slot="name">{{$t('File Name')}}</template>
            <template slot="content">
              <x-input
                type="input"
                v-model="name"
                :disabled="progress !== 0"
                :placeholder="$t('Please enter name')"
                autocomplete="off">
              </x-input>
            </template>
          </m-list-box-f>
        </div>
      </form>
    </template>
  </m-popup>
</template>
<script>
  import io from '@/module/io'
  import i18n from '@/module/i18n'
  import store from '@/conf/home/store'
  import mPopup from '@/module/components/popup/popup'
  import mListBoxF from '@/module/components/listBoxF/listBoxF'
  import mProgressBar from '@/module/components/progressBar/progressBar'

  export default {
    name: 'file-update',
    data () {
      return {
        store,
        // name
        name: '',
        // description
        description: '',
        // progress
        progress: 0,
        // file
        file: '',
        // Whether to drag upload
        dragOver: false
      }
    },
    watch: {
    },
    props: {
      type: String
    },
    methods: {
      /**
       * submit
       */
      _ok () {
        this.$refs['popup'].spinnerLoading = true
        if (this._validation()) {
          name: this.name
          this._formDataUpdate().then(res => {
            setTimeout(() => {
              this.$refs['popup'].spinnerLoading = false
            }, 800)
          }).catch(e => {
            this.$refs['popup'].spinnerLoading = false
          })
        } else {
          this.$refs['popup'].spinnerLoading = false
        }
      },
      /**
       * validation
       */
      _validation () {
        if (!this.file) {
          this.$message.warning(`${i18n.$t('Please select the file to upload')}`)
          return false
        }
        return true
      },
      /**
       * update file
       */
      _formDataUpdate () {
        return new Promise((resolve, reject) => {
          let self = this
          let formData = new FormData()
          formData.append('file', this.file)
          io.post(`projects/importProcessDefinition`, res => {
            this.$message.success(res.msg)
            resolve()
            self.$emit('onUpdate')
          }, e => {
            reject(e)
            self.$emit('close')
            this.$message.error(e.msg || '')
          }, {
            data: formData,
            emulateJSON: false,
            onUploadProgress (progressEvent) {
              // Size has been uploaded
              let loaded = progressEvent.loaded
              // Total attachment size
              let total = progressEvent.total
              self.progress = Math.floor(100 * loaded / total)
              self.$emit('onProgress', self.progress)
            }
          })
        })
      },
      /**
       * Archive to the top right corner Continue uploading
       */
      _ckArchive () {
        $('.update-file-modal').hide()
        this.$emit('onArchive')
      },
      /**
       * Drag and drop upload
       */
      _onDrop (e) {
        let file = e.dataTransfer.files[0]
        this.file = file
        this.name = file.name
        this.dragOver = false
      }
    },
    mounted () {
      $('#file').change(() => {
        let file = $('#file')[0].files[0]
        this.file = file
        this.name = file.name
      })
    },
    components: { mPopup, mListBoxF, mProgressBar }
  }
</script>

<style lang="scss" rel="stylesheet/scss">
  .file-update-model {
    .tooltip-info {
      position: absolute;
      left: 20px;
      bottom: 26px;
      span {
        font-size: 12px;
        color: #666;
        vertical-align: middle;
      }
      .fa {
        color: #0097e0;
        font-size: 14px;
        vertical-align: middle;
      }
    }
    .hide-archive {
      position: absolute;
      right: 22px;
      top: 17px;
      .fa{
        font-size: 16px;
        color: #333;
        font-weight: normal;
        cursor: pointer;
        &:hover {
          color: #0097e0;
        }
      }
    }
    .file-update-box {
      padding-top: 4px;
      position: relative;
      .file-update {
        width: 70px;
        height: 40px;
        position: absolute;
        left: 0;
        top: 0;
        cursor: pointer;
        filter: alpha(opacity=0);
        -moz-opacity: 0;
        opacity: 0;
      }
      &:hover {
        .v-btn-dashed {
          background-color: transparent;
          border-color: #47c3ff;
          color: #47c3ff;
          cursor: pointer;
        }
      }
      .progress-box {
        width: 200px;
        position: absolute;
        left: 70px;
        top: 14px;
      }
    }
    .update-popup {
      width: calc(100% - 20px);
      height: calc(100% - 20px);
      background: rgba(255,253,239,.7);
      position: absolute;
      top: 10px;
      left: 10px;
      border-radius: 3px;
      z-index: 1;
      border: .18rem dashed #cccccc;
      .icon-box {
        text-align: center;
        margin-top: 96px;
        .fa {
          font-size: 50px;
          color: #2d8cf0;
        }
      }
      .p1 {
        text-align: center;
        font-size: 16px;
        color: #333;
        padding-top: 8px;
      }
    }
  }
</style>
