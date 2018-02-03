'use strict';

const CommonImport = require('../../util/CommonImport');

class _CheckQualificationAndSaveMessageImpl {

  static checkQualificationAndSaveMessage(call, callback) {

    // call.request.conversationType === CommonImport.protos.enums.conversationTypes.GROUP
    const _getRealGroupTargetUsers = () => {
      return CommonImport.utils.bluebirdRetryExecutor(() => {
        const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
        const groupsCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.groupsCollectionName);
        return groupsCollection.findOne({
          groupId: call.request.forGroupId
        }, {
          fields: {
            members: 1,
            managers: 1,
            blockedMembers: 1
          }
        });
      }, {});
    }

    const _getOtherTypeTargetUsers = () => {
      return CommonImport.utils.bluebirdRetryExecutor(() => {
        const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
        const conversationsCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.conversationsCollectionName);
        return conversationsCollection.findOne({
          conversationId: call.request.toConversationId,
          conversationType: call.request.conversationType,
          [`members.${call.request.sender}`]: {
            $exists: true
          },
          conversationStatus: CommonImport.protos.enums.conversationStatuses.ACCESSIBLE
        }, {
          fields: {
            members: 1
          }
        });
      }, {});
    }

    const _getMentionedMessages = () => {
      return CommonImport.utils.bluebirdRetryExecutor(() => {
        const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
        const messagesCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.messagesCollectionName);
        return messagesCollection.find({
          messageId: {
            $in: call.request.mentionedMessageMessageIds
          },
          toConversationId: call.request.toConversationId
        }).sort({
          createAt: -1
        }).toArray();
      }, {});
    }

    const getItems = [];

    call.request.conversationType = CommonImport.protos.enums.conversationTypes[call.request.conversationType];
    call.request.messageType = CommonImport.protos.enums.messageTypes[call.request.messageType];

    if (call.request.conversationType === CommonImport.protos.enums.conversationTypes.GROUP) {
      getItems.push(_getRealGroupTargetUsers(), undefined);
    } else {
      getItems.push(undefined, _getOtherTypeTargetUsers());
    }

    if (Array.isArray(call.request.mentionedMessageMessageIds) && call.request.mentionedMessageMessageIds.length) {
      getItems.push(_getMentionedMessages());
    } else {
      getItems.push(undefined);
    }

    const messageId = CommonImport.uuidV4();

    CommonImport.Promise.all(getItems).then((res) => {

      const data = {
        messageId: messageId
      };

      let which;

      if (res[0]) {
        which = 0;
        
        let isSenderBlocked = false;
        if (res[0].blockedMembers && res[0].blockedMembers[call.request.sender]) {
          isSenderBlocked = true;
        }

        if (isSenderBlocked) {
          return CommonImport.Promise.reject(new CommonImport.errors.NoPermission.UserHasBeenBlocked());
        }

        data.aux = {
          managerUserIds: res[0].managers,
          blockedMembers: res[0].blockedMembers
        };
      } else if (res[1]) {
        which = 1;
      } else {
        return CommonImport.Promise.reject(new CommonImport.errors.ResourceNotFound.ConversationNotFound());
      }

      data.targetUsers = res[which].members;

      const targetUsersArr = Object.keys(data.targetUsers);

      let mentionedUserUserIds;
      if ((call.request.conversationType === CommonImport.protos.enums.conversationTypes.GROUP
            || call.request.conversationType === CommonImport.protos.enums.conversationTypes.TEMP_GROUP)
            && Array.isArray(call.request.mentionedUserUserIds) && call.request.mentionedUserUserIds.length) {
        mentionedUserUserIds = call.request.mentionedUserUserIds.filter((mentionedUserUserId) => {
          return targetUsersArr.indexOf(mentionedUserUserId) !== -1;
        });
      }

      if (mentionedUserUserIds) {
        data.mentionedUserUserIds = mentionedUserUserIds;
      }

      const now = Date.now();

      const message = {
        messageId: messageId,
        messageType: call.request.messageType,
        content: call.request.content,
        resources: call.request.resources,
        sender: call.request.sender,
        toConversationId: call.request.toConversationId,
        mentionedMessages: [],
        lastUpdate: now,
        createAt: now
      };

      if (data.mentionedUserUserIds) {
        message.mentionedUsers = data.mentionedUserUserIds;
      }

      if (res[2]) {
        if (res[2].length !== call.request.mentionedMessageMessageIds.length) {
          return CommonImport.Promise.reject(new CommonImport.errors.ResourceNotFound.MessageNotFound());
        }

        data.mentionedMessages = res[2].reduce((acc, curr) => {
          message.mentionedMessages.push(curr.messageId);
          const tmpMessageObj = {
            messageId: curr.messageId,
            messageType: curr.messageType,
            content: curr.content,
            urls: curr.urls,
            multi: curr.multi,
            sender: curr.sender,
            toConversationId: curr.toConversationId,
            mentionedUserUserIds: curr.mentionedUsers,
            mentionedMessageMessageIds: curr.mentionedMessages
          };
          acc.push(tmpMessageObj);
          return acc;
        }, []);
      }

      /*
       * Update user's `activeConversations` list.
       */
      const _updateUserActiveConversationsList = (targetUserUserIds, conversationId) => {
        const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
        const usersCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.usersCollectionName);
        return usersCollection.updateMany({
          userId: {
            $in: targetUserUserIds
          }
        }, {
          $addToSet: {
            activeConversations: conversationId
          }
        });
      };

      CommonImport.utils.cleanup(message);

      return CommonImport.Promise.join(
        CommonImport.Promise.resolve(
          CommonImport.utils.bluebirdRetryExecutor(() => {
            return _updateUserActiveConversationsList(targetUsersArr, message.toConversationId);
          }, {})
        ).reflect(),
        CommonImport.Promise.resolve(
          CommonImport.utils.bluebirdRetryExecutor(() => {
            const dbPool = CommonImport.utils.pickRandomly(global.DB_POOLS);
            const messagesCollection = dbPool.collection(global.RELATED_MONGODB_COLLECTIONS.messagesCollectionName);
            return messagesCollection.insertOne(message);
          }, {})
        ).reflect(),
        (updateUserActiveConversationsListInspection, saveMessageInspection) => {
          const isUpdateUserSuccessfull = updateUserActiveConversationsListInspection.isFulfilled();
          const isSavingMessageSuccessfull = saveMessageInspection.isFulfilled();
          if (!isUpdateUserSuccessfull && !isSavingMessageSuccessfull) {
            /*
             * Successfully did all the verification, but neither successfully updated the user's `activeConversations` list,
             * nor successfully saved the message.
             * 
             * TODO:
             *   Record corresponding scenes, use global task manager to do the continuously retry.
             */
            console.log(updateUserActiveConversationsListInspection.reason(), saveMessageInspection.reason());
          } else if (!isUpdateUserSuccessfull) {
            /* 
             * Successfully did all the verification, and also successfully saved the message,
             * but didn't successfully update the user's `activeConversations` list.
             *
             * TODO:
             *   Record corresponding scenes, use global task manager to do the continuously retry.
             */
            console.log(updateUserActiveConversationsListInspection.reason());
          } else if (!isSavingMessageSuccessfull) {
            /*
             * Successfully did all the verification, and also successfully updated the user's `activeConversations` list,
             * but didn't successfully save the message.
             *
             * TODO:
             *   Record corresponding scenes, use global task manager to do the continuously retry.
             */
            console.log(saveMessageInspection.reason());
          }

          return CommonImport.Promise.resolve(data);
        }
      );

    }).then((res) => {
      CommonImport.utils.cleanup(res);
      callback(null, res);
    }).catch((err) => {
      CommonImport.utils.apiImplCommonErrorHandler(err, CommonImport.errors, callback);
    });

  }

}

module.exports = _CheckQualificationAndSaveMessageImpl;


