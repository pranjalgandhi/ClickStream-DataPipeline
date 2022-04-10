package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{EVENT_TIMESTAMP, NULLKEY_CLICKSTREAM, NULLKEY_ITEM}
import com.igniteplus.data.pipeline.dqchecks.DqCheckMethods.{DqDuplicateCheck, DqNullCheck}
import com.igniteplus.data.pipeline.service.PipelineService.execute

object DqCheckPipelineService {
//      DqNullCheck(dfItemRemovedDuplicates,NULLKEY_ITEM)
//      DqNullCheck(dfViewLogLower,NULLKEY_CLICKSTREAM)
//
//      DqDuplicateCheck(dfItemRemovedDuplicates,NULLKEY_ITEM)
//      DqDuplicateCheck(dfViewLogLower,NULLKEY_CLICKSTREAM,EVENT_TIMESTAMP)
}
