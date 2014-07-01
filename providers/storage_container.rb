# Author Jeff Mendoza (jemendoz@microsoft.com)
#-------------------------------------------------------------------------
# Copyright (c) Microsoft Open Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------------

include Azure::Cookbook

action :create do
  setup_storage_service

  bms = Azure::BlobService.new
  cont_names = []
  bms.list_containers.each do |cont|
    cont_names.push(cont.name)
  end

  if cont_names.include?(new_resource.name)
    Chef::Log.debug("Blob container #{new_resource.name} already exists.")
  else
    Chef::Log.debug("Creating blob container #{new_resource.name}.")
    bms.create_container(new_resource.name)
  end
end

action :delete do
  setup_storage_service

  bms = Azure::BlobService.new
  cont_names = []
  bms.list_containers.each do |cont|
    cont_names.push(cont.name)
  end
  
  if cont_names.include?(new_resource.name)
    Chef::Log.debug("Deleting blob container #{new_resource.name}.")
    bms.delete_container(new_resource.name)
  else
    Chef::Log.debug("Blob container #{new_resource.name} does not exist.")
  end
end

MB=1024*1024
CHUNK_MAX=4*MB

action :retrieve do

  setup_storage_service
  abs = Azure::BlobService.new

  container_name=new_resource.container_name
  blob_name=new_resource.blob_name
  write_name=new_resource.local_filename

  content_length=-1
  # check if exists
  abs.list_blobs(container_name).each do |blob|
    if blob.name == blob_name
      content_length=blob.properties[:content_length]
      print "Target content length: #{content_length}\n"
    end
  end
  if content_length == -1
    "Content length -1 -- aborting"
    return
  end

  # if write_name exist, copy to .1?   or something
  if ::File.exists?(write_name)
    ::File.rename(write_name, "#{write_name}.1")
  end

  blob_pointer=0
  ::File.open(write_name, "wb") do |f|
    f.sync=true
    total_time=0.0
    while blob_pointer < content_length
      #loop
      # NOTE: 4MB is max size chunk azure can handle
      blob_end_pointer = blob_pointer + CHUNK_MAX
      #print "to block #{blob_end_pointer}\n"
      if blob_end_pointer > content_length
        blob_end_pointer = content_length
      end
      # do your work
      t1 = Time.now
      blob, content = abs.get_blob(container_name, blob_name)
      t2 = Time.now
      delta = t2 - t1
      total_time += delta
      printf "--- progress %.2f percent, time: %.2f s\n", (blob_end_pointer.to_f/content_length.to_f * 100).to_f, delta
      f.write(content)
      blob_pointer = blob_end_pointer+1
    end
    print "\nTotal dl time: %.2f seconds \n" % total_time
    print "Avg dl rate: %.4f  MBytes/sec\n" % (content_length.to_f/1024/1024/total_time.to_f)
  end
end

