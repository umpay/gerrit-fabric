/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"fmt"

	"github.com/hyperledger/fabric/core/peer"
	pb "github.com/hyperledger/fabric/protos"
	"github.com/spf13/cobra"
)

// UndefinedParamValue defines what undefined parameters in the command line will initialise to
const UndefinedParamValue = ""

// GetDevopsClient returns a new client connection for this peer
func GetDevopsClient(cmd *cobra.Command) (pb.DevopsClient, error) {
	clientConn, err := peer.NewPeerClientConnection()
	if err != nil {
		return nil, fmt.Errorf("Error trying to connect to local peer: %s", err)
	}
	devopsClient := pb.NewDevopsClient(clientConn)
	return devopsClient, nil
}

//Get memory information
func showMemory(desc string) string{
	calc := func (a uint64) string{
             var num,g float64
             g = 1024*1024*1024  //1G
             num = float64(a)
             if num < g {
                 return fmt.Sprintf("%.2f%s",num / g * 1024.0,"M")
             }else {
                 return fmt.Sprintf("%.2f%s",num / g,"G")
             }
        }
	var mem runtime.MemStats
    runtime.ReadMemStats(&mem)
    return fmt.Sprintf("%s Alloc:%s TotalAlloc:%s HeapAlloc:%s HeapSys:%s"
    			,desc,calc(mem.Alloc),calc(mem.TotalAlloc),calc(mem.HeapAlloc),calc(mem.HeapSys))
}