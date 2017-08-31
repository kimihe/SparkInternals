README

把“BlockTransferService.scala”放到对应的目录下，替换掉原始文件，然后编译。
运行一个例子，如wordcount，观察控制台的输出，看是否有打印出中间数据的信息。
信息格式如下：
println("****************************** [Fetch Data] ******************************\n" +
            "DataSize:  ${data.size.toInt}\n" + 
            "Host:      $host\n" + 
            "Port:      $port\n" +
            "ExecId:    $execId\n" +
            "BlockId:   $blockId\n")