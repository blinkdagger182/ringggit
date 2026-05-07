//
//  HomeAIAssistantOverlay.swift
//  dime
//

import MarkdownUI
import Popovers
import SwiftUI
import UIKit

struct HomeAIAssistantOverlay: View {
    private let assistantTitle = "Saku AI"

    @ObservedObject var viewModel: HomeAIAssistantViewModel
    @StateObject private var keyboardHeightHelper = KeyboardHeightHelper()

    let namespace: Namespace.ID
    let topInset: CGFloat
    let bottomInset: CGFloat
    let revealProgress: CGFloat
    let isExpanded: Bool
    let homePeekHeight: CGFloat
    let onCollapse: () -> Void
    let collapseGesture: AnyGesture<DragGesture.Value>?

    @State private var composerFocused = false
    @State private var composerFocusRequestID = 0
    @State private var chatScrollRequestID = 0
    @State private var showAttachmentSheet = false
    @State private var showWorkspaceMenu = false
    @State private var previewAttachment: AttachmentItem?
    private let landingActions: [SakuAILandingAction] = [
        .init(title: "What can Saku AI do?", systemImage: "sparkles", prompt: "What can Saku AI do?"),
        .init(title: "Scan receipt", systemImage: "doc.text.viewfinder", prompt: nil),
        .init(title: "Add transaction", systemImage: "plus.circle", prompt: "Add transaction"),
        .init(title: "Where did my money go?", systemImage: "chart.pie", prompt: "Where did my money go?"),
        .init(title: "Upload file", systemImage: "folder", prompt: nil),
        .init(title: "Show latest transfers", systemImage: "arrow.left.arrow.right", prompt: "Show latest transfers")
    ]

    var body: some View {
        GeometryReader { proxy in
            let keyboardOverlap = max(0, keyboardHeightHelper.keyboardHeight - proxy.safeAreaInsets.bottom)
            let visibleHomePadding = isExpanded && keyboardOverlap == 0 ? homePeekHeight : 0
            let baseComposerBottomPadding = max(proxy.safeAreaInsets.bottom, bottomInset) + 12 + visibleHomePadding
            let activeComposerBottomPadding = keyboardOverlap > 0 ? 12 : baseComposerBottomPadding

            ZStack(alignment: .bottom) {
                SakuAIDarkBackgroundView()
                    .ignoresSafeArea()
                    .opacity(max(0.28, revealProgress))

                VStack(spacing: 0) {
                    previewHeader
                        .padding(.horizontal, 24)
                        .padding(.top, max(topInset, proxy.safeAreaInsets.top) + 6)
                        .opacity(revealProgress)

                    messageArea(
                        keyboardOverlap: keyboardOverlap,
                        composerBottomPadding: activeComposerBottomPadding
                    )
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                        .opacity(revealProgress)
                        .offset(y: (1 - revealProgress) * 24)
                }
                .frame(width: proxy.size.width, height: proxy.size.height, alignment: .top)

                composerSection
                    .padding(.bottom, activeComposerBottomPadding)
                    .opacity(revealProgress)
                    .offset(y: (1 - revealProgress) * 20 - keyboardOverlap)
                    .background(
                        LinearGradient(
                            colors: [
                                Color(hex: "0B1023").opacity(0),
                                Color(hex: "0B1023").opacity(0.86),
                                Color(hex: "0B1023")
                            ],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                    )
                    .zIndex(2)

                if isExpanded && keyboardOverlap == 0 {
                    bottomHomePeek(safeBottom: max(proxy.safeAreaInsets.bottom, bottomInset))
                        .opacity(revealProgress)
                        .zIndex(3)
                }
            }
            .ignoresSafeArea(.keyboard, edges: .bottom)
            .onChange(of: isExpanded) { expanded in
                guard expanded else { return }
                guard viewModel.messages.isEmpty else { return }

                DispatchQueue.main.asyncAfter(deadline: .now() + 0.38) {
                    guard isExpanded else { return }
                    focusComposer()
                }
            }
        }
        .fullScreenCover(item: $previewAttachment) { item in
            AttachmentPreviewView(item: item)
        }
        .sheet(isPresented: $showAttachmentSheet) {
            let sheet = HomeAIAttachmentSheet(
                attachments: $viewModel.pendingAttachments,
                onQuickPrompt: { prompt in
                    viewModel.draftMessage = prompt
                    viewModel.sendDraftMessage()
                }
            )
            if #available(iOS 16.0, *) {
                sheet
                    .presentationDetents([.medium])
                    .presentationDragIndicator(.hidden)
            } else {
                sheet
            }
        }
    }

    private var previewHeader: some View {
        VStack(spacing: 10) {
            ZStack {
                HStack(spacing: 8) {
                    Text("✨ \(assistantTitle)")
                        .font(.system(size: 22, weight: .semibold))
                        .foregroundColor(.white)

                    Text("beta")
                        .font(.system(size: 12, design: .rounded).weight(.medium))
                        .foregroundColor(Color.white.opacity(0.78))
                        .padding(.horizontal, 9)
                        .padding(.vertical, 4)
                        .overlay(
                            Capsule(style: .continuous)
                                .stroke(Color.white.opacity(0.24), lineWidth: 1)
                        )
                }
            }
            .frame(height: 44)

            Button {
                showWorkspaceMenu = true
            } label: {
                HStack(spacing: 4) {
                    Text(viewModel.activeWorkspace.title)
                        .font(.system(.body, design: .rounded).weight(.medium))

                    Image(systemName: "chevron.up.chevron.down")
                        .font(.system(.caption, design: .rounded).weight(.medium))
                }
                .padding(.horizontal, 12)
                .padding(.vertical, 6)
                .foregroundColor(activeWorkspaceTint)
                .background(
                    activeWorkspaceBackground,
                    in: Capsule(style: .continuous)
                )
            }
            .buttonStyle(.plain)
            .popover(present: $showWorkspaceMenu, attributes: {
                $0.position = .absolute(
                    originAnchor: .bottom,
                    popoverAnchor: .top
                )
                $0.rubberBandingMode = .none
                $0.sourceFrameInset = UIEdgeInsets(top: 0, left: 0, bottom: -10, right: 0)
                $0.presentation.animation = .easeInOut(duration: 0.2)
                $0.dismissal.animation = .easeInOut(duration: 0.3)
            }) {
                HomeAIWorkspacePickerView(
                    workspaces: viewModel.workspaces,
                    activeWorkspaceID: viewModel.activeWorkspaceID,
                    activeBucketID: viewModel.activeWorkspace.bucketID,
                    onSelect: { workspaceID in
                        viewModel.selectWorkspace(workspaceID)
                        showWorkspaceMenu = false
                    }
                )
            }
        }
        .frame(maxWidth: .infinity)
    }

    private var activeWorkspaceTint: Color {
        if let bucket = viewModel.activeBucket {
            return Color(hex: bucket.colour ?? "7B8FF8")
        }
        return Color(hex: "4ECDC4")
    }

    private var activeWorkspaceBackground: Color {
        if viewModel.activeWorkspace.bucketID == nil {
            return Color.white.opacity(0.10)
        }
        return activeWorkspaceTint.opacity(0.20)
    }

    private func messageArea(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> some View {
        ScrollViewReader { proxy in
            ScrollView(showsIndicators: false) {
                LazyVStack(alignment: .leading, spacing: 16) {
                    if viewModel.messages.isEmpty {
                        emptyState
                    }

                    ForEach(viewModel.messages) { message in
                        messageBubble(message)
                            .id(message.id)
                    }

                    Color.clear
                        .frame(
                            maxWidth: .infinity,
                            minHeight: bottomScrollSpacerHeight(
                                keyboardOverlap: keyboardOverlap,
                                composerBottomPadding: composerBottomPadding
                            ),
                            maxHeight: bottomScrollSpacerHeight(
                                keyboardOverlap: keyboardOverlap,
                                composerBottomPadding: composerBottomPadding
                            )
                        )
                        .contentShape(Rectangle())
                        .onTapGesture {
                            dismissKeyboard()
                        }
                        .id("bottom-anchor")
                }
                .background(
                    Color.black.opacity(0.001)
                        .contentShape(Rectangle())
                        .onTapGesture {
                            dismissKeyboard()
                        }
                )
                .padding(.horizontal, 24)
                .padding(.top, 16)
            }
            .modifier(HomeAIInteractiveKeyboardDismissModifier())
            .background(
                nonBubbleDragArea(height: 1)
            )
            .onChange(of: viewModel.messages.count) { _ in
                requestScrollToChatEnd(proxy, animated: false)
            }
            .onChange(of: viewModel.streamingText) { _ in
                guard !viewModel.streamingText.isEmpty else { return }
                requestScrollToChatEnd(proxy, animated: true)
            }
            .onChange(of: viewModel.messageContentRevision) { _ in
                guard !viewModel.isStreaming else { return }
                requestScrollToChatEnd(proxy, animated: false, delay: 0.02)
            }
            .onChange(of: keyboardHeightHelper.keyboardHeight) { _ in
                requestScrollToChatEnd(proxy, animated: false, delay: 0.18)
            }
            .onAppear {
                guard !viewModel.messages.isEmpty else { return }
                requestScrollToChatEnd(proxy, animated: false, delay: 0)
            }
        }
    }

    private func nonBubbleDragArea(height: CGFloat) -> some View {
        Color.black.opacity(0.001)
            .frame(maxWidth: .infinity, minHeight: height, maxHeight: height)
            .contentShape(Rectangle())
            .onTapGesture {
                dismissKeyboard()
            }
    }

    private var emptyState: some View {
        VStack(spacing: 28) {
            VStack(spacing: 14) {
                Text("How can I help you?")
                    .font(.system(size: 31, weight: .semibold, design: .rounded))
                    .foregroundColor(.white)
                    .multilineTextAlignment(.center)

                landingActionsGrid
            }

            Text("Built for your money, privately.")
                .font(.system(.footnote, design: .rounded).weight(.medium))
                .foregroundColor(Color.white.opacity(0.56))
        }
        .padding(.top, 44)
    }

    private var landingActionsGrid: some View {
        LazyVGrid(
            columns: [
                GridItem(.flexible(), spacing: 14),
                GridItem(.flexible(), spacing: 14)
            ],
            spacing: 14
        ) {
            ForEach(landingActions) { action in
                Button {
                    handleLandingAction(action)
                } label: {
                    HStack(spacing: 10) {
                        Image(systemName: action.systemImage)
                            .font(.system(size: 16, weight: .semibold))
                            .foregroundColor(.white)
                            .frame(width: 22)

                        Text(action.title)
                            .font(.system(.body, design: .rounded).weight(.medium))
                            .foregroundColor(.white)
                            .multilineTextAlignment(.leading)

                        Spacer(minLength: 0)
                    }
                    .padding(.horizontal, 16)
                    .frame(maxWidth: .infinity, minHeight: 72, alignment: .leading)
                    .background(
                        RoundedRectangle(cornerRadius: 20, style: .continuous)
                            .fill(Color.white.opacity(0.09))
                            .overlay(
                                RoundedRectangle(cornerRadius: 20, style: .continuous)
                                    .stroke(Color.white.opacity(0.10), lineWidth: 1)
                            )
                    )
                }
                .buttonStyle(.plain)
            }
        }
    }

    private func quickActionsSection(title: String) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            if !title.isEmpty {
                Text(title)
                    .font(.system(.subheadline).weight(.medium))
                    .foregroundColor(Color.white.opacity(0.62))
                    .padding(.horizontal, 2)
            }

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 12) {
                    ForEach(viewModel.quickActions) { quickAction in
                        Button {
                            viewModel.triggerQuickAction(quickAction)
                        } label: {
                            VStack(alignment: .leading, spacing: 10) {
                                ZStack {
                                    Circle()
                                        .fill(
                                            LinearGradient(
                                                colors: [Color(hex: "4ECDC4").opacity(0.18), Color(hex: "9BAAF8").opacity(0.22)],
                                                startPoint: .topLeading, endPoint: .bottomTrailing
                                            )
                                        )
                                        .frame(width: 34, height: 34)
                                    Image(systemName: quickAction.systemImage)
                                        .font(.system(size: 14, weight: .medium))
                                        .foregroundStyle(
                                            LinearGradient(
                                                colors: [Color(hex: "4ECDC4"), Color(hex: "7B8FF8")],
                                                startPoint: .topLeading, endPoint: .bottomTrailing
                                            )
                                        )
                                }

                                Spacer(minLength: 0)

                                Text(quickAction.title)
                                    .font(.system(.subheadline).weight(.semibold))
                                    .foregroundColor(.white)
                                    .lineLimit(2)
                                    .multilineTextAlignment(.leading)

                                Text(quickActionSubtitle(for: quickAction.title))
                                    .font(.system(.caption, design: .rounded))
                                    .foregroundColor(Color.white.opacity(0.56))
                                    .lineLimit(2)
                            }
                            .padding(14)
                            .frame(width: 148, height: 148, alignment: .leading)
                            .background(
                                RoundedRectangle(cornerRadius: 16, style: .continuous)
                                    .fill(Color.white.opacity(0.08))
                                    .overlay(
                                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                                            .stroke(Color.white.opacity(0.08), lineWidth: 1)
                                    )
                            )
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.trailing, 24)
                .padding(.bottom, 4)
            }
            .scrollClipDisabledIfAvailable()
        }
    }

    private func messageBubble(_ message: HomeAIMessage) -> some View {
        HStack(alignment: .top) {
            if message.role == .assistant {
                VStack(alignment: .leading, spacing: 10) {
                    HStack(alignment: .top, spacing: 10) {
                        ZStack {
                            Circle()
                                .fill(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4").opacity(0.18), Color(hex: "9BAAF8").opacity(0.22)],
                                        startPoint: .topLeading, endPoint: .bottomTrailing
                                    )
                                )
                                .frame(width: 30, height: 30)
                            Image(systemName: "sparkles")
                                .font(.system(size: 13, weight: .semibold))
                                .foregroundStyle(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4"), Color(hex: "7B8FF8")],
                                        startPoint: .topLeading, endPoint: .bottomTrailing
                                    )
                                )
                        }
                        .padding(.top, 2)

                        VStack(alignment: .leading, spacing: 8) {
                            Text("Saku AI")
                                .font(.system(.subheadline).weight(.semibold))
                                .foregroundStyle(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4"), Color(hex: "7B8FF8")],
                                        startPoint: .leading, endPoint: .trailing
                                    )
                                )

                            // ThinkingDisclosure commented out
                            // if let thinking = message.thinking {
                            //     ThinkingDisclosure(thinking: thinking)
                            // }

                            if !message.text.isEmpty {
                                HomeAIMarkdownText(text: message.text)
                            } else if isStreamingAssistantPlaceholder(message) {
                                HStack(spacing: 8) {
                                    Text(viewModel.statusText.isEmpty ? "Thinking" : viewModel.statusText)
                                        .font(.system(.body))
                                        .foregroundColor(Color.white.opacity(0.82))
                                        .lineSpacing(5)
                                    AIThinkingDots()
                                }
                            }
                        }
                    }

                    if let visual = message.visual {
                        AIVisualCardView(visual: visual)
                    }
                }

                Spacer(minLength: 42)
            } else {
                Spacer(minLength: 42)

                let hasText = !message.text.isEmpty && message.text != "Please extract all transactions from the attached image(s)."
                let firstAttachment = message.attachments.first

                VStack(alignment: .trailing, spacing: 0) {
                    if firstAttachment != nil || hasText {
                        VStack(alignment: .leading, spacing: 0) {
                            if let attachment = firstAttachment {
                                Button { previewAttachment = attachment } label: {
                                    Image(uiImage: attachment.thumbnail)
                                        .resizable()
                                        .scaledToFill()
                                        .frame(width: 240, height: 170)
                                        .clipped()
                                        .overlay(alignment: .bottomLeading) {
                                            if attachment.pages.count > 1 {
                                                Text(attachment.label.components(separatedBy: "·").last?.trimmingCharacters(in: .whitespaces) ?? "")
                                                    .font(.system(size: 10, weight: .semibold, design: .rounded))
                                                    .foregroundColor(.white)
                                                    .padding(.horizontal, 6)
                                                    .padding(.vertical, 3)
                                                    .background(Color.black.opacity(0.55))
                                                    .clipShape(Capsule())
                                                    .padding(6)
                                            }
                                        }
                                }
                                .buttonStyle(.plain)
                            }

                            if hasText {
                                Text(message.text)
                                    .font(.system(.body).weight(.medium))
                                    .foregroundColor(.white)
                                    .padding(.horizontal, 14)
                                    .padding(.vertical, 11)
                            }
                        }
                        .background(
                            LinearGradient(
                                colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                        .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
                        .frame(maxWidth: 260, alignment: .trailing)
                    }
                }
            }
        }
    }

    private func isStreamingAssistantPlaceholder(_ message: HomeAIMessage) -> Bool {
        message.role == .assistant
            && message.id == viewModel.messages.last?.id
            && message.text.isEmpty
            && viewModel.isStreaming
    }

    private var composerSection: some View {
        VStack(spacing: 10) {
            if !viewModel.isStreaming {
                composerQuickPrompts
            }

            HStack(alignment: .bottom, spacing: 10) {
                if keyboardHeightHelper.keyboardHeight > 0 {
                    Button { showAttachmentSheet = true } label: {
                        ZStack {
                            Circle()
                                .fill(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                        startPoint: .topLeading, endPoint: .bottomTrailing
                                    )
                                )
                                .frame(width: 50, height: 50)
                            Image(systemName: "plus")
                                .font(.system(size: 20, weight: .medium))
                                .foregroundColor(.white)
                        }
                    }
                    .buttonStyle(.plain)
                    .padding(.bottom, 3)
                }

                composer
            }
        }
        .padding(.horizontal, 20)
    }

    private var composerQuickPrompts: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                ForEach(viewModel.quickActions) { quickAction in
                    Button {
                        viewModel.triggerQuickAction(quickAction)
                    } label: {
                        HStack(spacing: 6) {
                            Image(systemName: quickAction.systemImage)
                                .font(.system(size: 11, weight: .semibold))
                            Text(quickAction.title)
                                .font(.system(.footnote, design: .rounded).weight(.medium))
                                .lineLimit(1)
                        }
                        .foregroundColor(Color.white.opacity(0.92))
                        .padding(.horizontal, 12)
                        .padding(.vertical, 8)
                        .background(Color.white.opacity(0.10), in: Capsule(style: .continuous))
                        .overlay(
                            Capsule(style: .continuous)
                                .stroke(Color.white.opacity(0.10), lineWidth: 1)
                        )
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.horizontal, 2)
        }
        .scrollClipDisabledIfAvailable()
    }

    private var composer: some View {
        VStack(spacing: 0) {
            // Attachment thumbnails inside the pill
            if !viewModel.pendingAttachments.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(viewModel.pendingAttachments) { item in
                            composerAttachmentCell(item)
                        }
                    }
                    .padding(.horizontal, 14)
                    .padding(.vertical, 10)
                }
                .frame(height: 90)

                Rectangle()
                    .fill(Color(.separator).opacity(0.5))
                    .frame(height: 1)
                    .padding(.horizontal, 14)
            }

            VStack(spacing: 12) {
                HStack(alignment: .center, spacing: 12) {
                    HomeAIComposerTextField(
                        text: $viewModel.draftMessage,
                        isFocused: composerFocusBinding,
                        focusRequestID: composerFocusRequestID,
                        placeholder: "Ask Saku AI",
                        onSubmit: sendComposerMessage,
                        textColor: .white,
                        placeholderColor: UIColor.white.withAlphaComponent(0.48)
                    )
                    .frame(maxWidth: .infinity, minHeight: 24, maxHeight: 24, alignment: .leading)

                    if viewModel.hasContent {
                        Button { sendComposerMessage() } label: {
                            Image(systemName: "arrow.up.circle.fill")
                                .font(.system(size: 26, weight: .semibold))
                                .foregroundStyle(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                        startPoint: .topLeading,
                                        endPoint: .bottomTrailing
                                    )
                                )
                        }
                        .buttonStyle(.plain)
                    }
                }
                .frame(height: 24)

                HStack(spacing: 20) {
                    Button { showAttachmentSheet = true } label: {
                        Image(systemName: "plus")
                            .font(.system(size: 18, weight: .medium))
                            .foregroundColor(.white.opacity(0.92))
                    }
                    .buttonStyle(.plain)

                    Button { showAttachmentSheet = true } label: {
                        Image(systemName: "photo")
                            .font(.system(size: 18, weight: .medium))
                            .foregroundColor(.white.opacity(0.92))
                    }
                    .buttonStyle(.plain)

                    Button {
                        viewModel.draftMessage = viewModel.draftMessage + "@"
                        focusComposer()
                    } label: {
                        Image(systemName: "at")
                            .font(.system(size: 18, weight: .medium))
                            .foregroundColor(.white.opacity(0.92))
                    }
                    .buttonStyle(.plain)

                    Button {
                        if viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                            viewModel.draftMessage = "What can Saku AI do?"
                        }
                        focusComposer()
                    } label: {
                        Image(systemName: "sparkles")
                            .font(.system(size: 18, weight: .medium))
                            .foregroundColor(.white.opacity(0.92))
                    }
                    .buttonStyle(.plain)

                    Spacer(minLength: 0)
                }
                .frame(height: 20)
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 16)
        }
        .contentShape(Rectangle())
        .onTapGesture { focusComposer() }
        .background(
            RoundedRectangle(cornerRadius: 24, style: .continuous)
                .fill(Color.white.opacity(0.10))
                .overlay(
                    RoundedRectangle(cornerRadius: 24, style: .continuous)
                        .stroke(composerFocused ? Color.white.opacity(0.24) : Color.white.opacity(0.10), lineWidth: 1)
                )
                .shadow(color: Color.black.opacity(composerFocused ? 0.22 : 0.14), radius: composerFocused ? 24 : 14, y: 8)
        )
        .simultaneousGesture(
            DragGesture(minimumDistance: 4)
                .onChanged { value in
                    guard value.translation.height > 8 else { return }
                    dismissKeyboard()
                }
        )
    }

    private func composerAttachmentCell(_ item: AttachmentItem) -> some View {
        ZStack(alignment: .topTrailing) {
            Button { previewAttachment = item } label: {
                ZStack(alignment: .bottomLeading) {
                    Image(uiImage: item.thumbnail)
                        .resizable()
                        .scaledToFill()
                        .frame(width: 68, height: 68)
                        .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

                    if item.pages.count > 1 {
                        Text(item.label.components(separatedBy: "·").last?.trimmingCharacters(in: .whitespaces) ?? "")
                            .font(.system(size: 9, weight: .semibold, design: .rounded))
                            .foregroundColor(.white)
                            .padding(.horizontal, 5)
                            .padding(.vertical, 2)
                            .background(Color.black.opacity(0.6))
                            .clipShape(Capsule())
                            .padding(4)
                    }
                }
            }
            .buttonStyle(.plain)

            Button { viewModel.removeAttachment(id: item.id) } label: {
                ZStack {
                    Circle()
                        .fill(Color(hex: "2C2C2C"))
                        .frame(width: 20, height: 20)
                    Image(systemName: "xmark")
                        .font(.system(size: 9, weight: .bold))
                        .foregroundColor(.white)
                }
                .padding(3)
            }
            .buttonStyle(.plain)
        }
    }

    private func bottomHomePeek(safeBottom: CGFloat) -> some View {
        let visibleHeight = max(homePeekHeight, 86)
        let hiddenDepth: CGFloat = max(120, safeBottom + 96)
        let panelHeight = visibleHeight + hiddenDepth

        return VStack(spacing: 0) {
            VStack(spacing: 0) {
                Image(systemName: "chevron.up")
                    .font(.system(size: 17, weight: .semibold))
                    .foregroundColor(Color(.quaternaryLabel))
                    .padding(.top, 14)

                Spacer(minLength: 0)
            }
            .frame(maxWidth: .infinity)
            .frame(height: panelHeight)
            .background(
                Color.PrimaryBackground
                    .overlay(alignment: .top) {
                        LinearGradient(
                            colors: [Color.white.opacity(0.30), Color.white.opacity(0)],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                        .frame(height: 18)
                    }
            )
            .clipShape(BottomHomePeekShape(cornerRadius: 34))
            .offset(y: hiddenDepth)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .bottom)
        .contentShape(Rectangle())
        .ignoresSafeArea(edges: .bottom)
        .onTapGesture(perform: onCollapse)
        .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))
        .accessibilityLabel("Swipe up to return Home")
    }

    private func requestScrollToChatEnd(_ proxy: ScrollViewProxy, animated: Bool, delay: TimeInterval = 0) {
        chatScrollRequestID += 1
        let requestID = chatScrollRequestID

        let performScroll = {
            guard requestID == chatScrollRequestID else { return }

            let action = {
                proxy.scrollTo("bottom-anchor", anchor: .bottom)
            }

            if animated {
                withAnimation(.linear(duration: 0.08)) {
                    action()
                }
            } else {
                action()
            }
        }

        if delay == 0 {
            DispatchQueue.main.async(execute: performScroll)
        } else {
            DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: performScroll)
        }
    }

    private func sendComposerMessage() {
        guard !viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard !viewModel.isStreaming else { return }

        viewModel.sendDraftMessage()
    }

    private func dismissKeyboard() {
        composerFocused = false
        UIApplication.shared.endEditing()
    }

    private func bottomScrollSpacerHeight(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> CGFloat {
        72 + composerBottomPadding + keyboardOverlap
    }

    private func focusComposer() {
        composerFocused = true
        composerFocusRequestID += 1
    }

    private func quickActionSubtitle(for title: String) -> String {
        switch title {
        case "Add transaction":
            return "Quickly log an expense"
        case "Where did my money go?":
            return "Breakdown by category"
        case "Show this month's spending":
            return "Review this month's flow"
        case "Can I afford this?":
            return "Check against current budget"
        default:
            return "Start with a suggested prompt"
        }
    }

    private var composerFocusBinding: Binding<Bool> {
        Binding(
            get: { composerFocused },
            set: { composerFocused = $0 }
        )
    }

    private func handleLandingAction(_ action: SakuAILandingAction) {
        switch action.title {
        case "Scan receipt", "Upload file":
            showAttachmentSheet = true
        default:
            if let quickAction = viewModel.quickActions.first(where: { $0.title == action.title }) {
                viewModel.triggerQuickAction(quickAction)
            } else if let prompt = action.prompt {
                viewModel.draftMessage = prompt
                viewModel.sendDraftMessage()
            }
        }
    }
}

private struct AttachmentPreviewView: View {
    let item: AttachmentItem
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Color.black.ignoresSafeArea()

            if item.pages.count == 1 {
                Image(uiImage: item.pages[0])
                    .resizable()
                    .scaledToFit()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                TabView {
                    ForEach(item.pages.indices, id: \.self) { i in
                        Image(uiImage: item.pages[i])
                            .resizable()
                            .scaledToFit()
                            .frame(maxWidth: .infinity, maxHeight: .infinity)
                            .tag(i)
                    }
                }
                .tabViewStyle(.page)
                .indexViewStyle(.page(backgroundDisplayMode: .always))
            }

            Button { dismiss() } label: {
                ZStack {
                    Circle()
                        .fill(Color.white.opacity(0.15))
                        .frame(width: 36, height: 36)
                    Image(systemName: "xmark")
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundColor(.white)
                }
            }
            .buttonStyle(.plain)
            .padding(.top, 56)
            .padding(.trailing, 20)
        }
    }
}

private struct HomeAICollapseGestureModifier: ViewModifier {
    let dragGesture: AnyGesture<DragGesture.Value>?

    func body(content: Content) -> some View {
        if let dragGesture {
            content.highPriorityGesture(dragGesture)
        } else {
            content
        }
    }
}

private struct BottomHomePeekShape: Shape {
    let cornerRadius: CGFloat

    func path(in rect: CGRect) -> Path {
        let path = UIBezierPath(
            roundedRect: rect,
            byRoundingCorners: [.topLeft, .topRight],
            cornerRadii: CGSize(width: cornerRadius, height: cornerRadius)
        )
        return Path(path.cgPath)
    }
}

private struct HomeAIInteractiveKeyboardDismissModifier: ViewModifier {
    func body(content: Content) -> some View {
        if #available(iOS 16.0, *) {
            content.scrollDismissesKeyboard(.interactively)
        } else {
            content
        }
    }
}

private extension View {
    @ViewBuilder
    func scrollClipDisabledIfAvailable() -> some View {
        if #available(iOS 17.0, *) {
            self.scrollClipDisabled()
        } else {
            self
        }
    }
}

private struct HomeAIComposerTextField: UIViewRepresentable {
    @Binding var text: String
    @Binding var isFocused: Bool

    let focusRequestID: Int
    let placeholder: String
    let onSubmit: () -> Void
    let textColor: UIColor
    let placeholderColor: UIColor

    func makeUIView(context: Context) -> UITextField {
        let textField = UITextField(frame: .zero)
        textField.delegate = context.coordinator
        textField.backgroundColor = .clear
        textField.borderStyle = .none
        textField.returnKeyType = .send
        textField.textColor = textColor
        textField.tintColor = UIColor(red: 0.306, green: 0.804, blue: 0.769, alpha: 1)
        textField.font = UIFont.roundedSystemFont(ofSize: 17, weight: .regular)
        textField.autocorrectionType = .yes
        textField.autocapitalizationType = .sentences
        textField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        textField.addTarget(context.coordinator, action: #selector(Coordinator.textDidChange(_:)), for: .editingChanged)
        updatePlaceholder(for: textField)
        return textField
    }

    func updateUIView(_ textField: UITextField, context: Context) {
        context.coordinator.parent = self

        if textField.text != text {
            textField.text = text
        }

        updatePlaceholder(for: textField)

        if context.coordinator.lastFocusRequestID != focusRequestID {
            context.coordinator.lastFocusRequestID = focusRequestID
            DispatchQueue.main.async {
                textField.becomeFirstResponder()
            }
        } else if isFocused && !textField.isFirstResponder {
            DispatchQueue.main.async {
                textField.becomeFirstResponder()
            }
        } else if !isFocused && textField.isFirstResponder {
            textField.resignFirstResponder()
        }

        textField.textColor = textColor
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    private func updatePlaceholder(for textField: UITextField) {
        textField.attributedPlaceholder = NSAttributedString(
            string: placeholder,
            attributes: [
                .foregroundColor: placeholderColor,
                .font: UIFont.roundedSystemFont(ofSize: 17, weight: .regular)
            ]
        )
    }

    final class Coordinator: NSObject, UITextFieldDelegate {
        var parent: HomeAIComposerTextField
        var lastFocusRequestID: Int

        init(parent: HomeAIComposerTextField) {
            self.parent = parent
            self.lastFocusRequestID = parent.focusRequestID
        }

        @objc func textDidChange(_ textField: UITextField) {
            parent.text = textField.text ?? ""
        }

        func textFieldDidBeginEditing(_ textField: UITextField) {
            parent.isFocused = true
        }

        func textFieldDidEndEditing(_ textField: UITextField) {
            parent.isFocused = false
        }

        func textFieldShouldReturn(_ textField: UITextField) -> Bool {
            parent.onSubmit()
            return false
        }
    }
}

private extension UIFont {
    static func roundedSystemFont(ofSize size: CGFloat, weight: UIFont.Weight) -> UIFont {
        let systemFont = UIFont.systemFont(ofSize: size, weight: weight)
        guard let descriptor = systemFont.fontDescriptor.withDesign(.rounded) else {
            return systemFont
        }
        return UIFont(descriptor: descriptor, size: size)
    }
}

private struct AIThinkingDots: View {
    @State private var phase = 0

    var body: some View {
        HStack(spacing: 4) {
            ForEach(0..<3, id: \.self) { i in
                Circle()
                    .fill(Color.white.opacity(0.48))
                    .frame(width: 5, height: 5)
                    .scaleEffect(phase == i ? 1.4 : 1.0)
                    .animation(.easeInOut(duration: 0.4).repeatForever(autoreverses: true).delay(Double(i) * 0.15), value: phase)
            }
        }
        .onAppear {
            phase = 0
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) { phase = 2 }
        }
    }
}

private struct HomeAIMarkdownText: View {
    let text: String

    var body: some View {
        Markdown(text)
        .markdownTheme(.gitHub)
        .markdownTextStyle(\.text) {
            FontSize(.em(1.0))
            ForegroundColor(.white.opacity(0.86))
            BackgroundColor(nil)
        }
        .markdownTextStyle(\.strong) {
            FontWeight(.semibold)
        }
        .markdownTextStyle(\.code) {
            FontFamilyVariant(.monospaced)
            FontSize(.em(0.92))
            BackgroundColor(nil)
        }
        .markdownBlockStyle(\.paragraph) { configuration in
            configuration.label
                .relativeLineSpacing(.em(0.22))
                .markdownMargin(top: 0, bottom: 14)
        }
        .markdownBlockStyle(\.codeBlock) { configuration in
            ScrollView(.horizontal, showsIndicators: false) {
                configuration.label
                    .padding(.horizontal, 12)
                    .padding(.vertical, 10)
            }
            .background(Color.clear)
            .overlay(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(Color(hex: "4ECDC4").opacity(0.22), lineWidth: 1)
            )
            .markdownMargin(top: 6, bottom: 16)
        }
        .markdownBlockStyle(\.blockquote) { configuration in
            configuration.label
                .padding(.leading, 14)
                .padding(.vertical, 2)
                .markdownTextStyle {
                    ForegroundColor(.white.opacity(0.62))
                }
                .overlay(alignment: .leading) {
                    RoundedRectangle(cornerRadius: 999, style: .continuous)
                        .fill(Color(hex: "4ECDC4").opacity(0.45))
                        .frame(width: 3)
                }
                .markdownMargin(top: 4, bottom: 16)
        }
        .tint(Color(hex: "4ECDC4"))
        .textSelection(.enabled)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.clear)
    }
}

private struct HomeAIWorkspacePickerView: View {
    let workspaces: [AIConversationWorkspace]
    let activeWorkspaceID: String
    let activeBucketID: UUID?
    let onSelect: (String) -> Void

    @Environment(\.colorScheme) private var colorScheme
    @Namespace private var animation

    private var darkMode: Bool {
        colorScheme == .dark
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 1) {
            ForEach(workspaces) { workspace in
                HStack(spacing: 10) {
                    Image(systemName: workspace.bucketID == nil ? "sparkles" : "flag.fill")
                        .font(.system(size: 13, weight: .semibold))
                        .foregroundColor(workspaceTint(workspace))
                        .frame(width: 22)

                    Text(workspace.title)
                        .lineLimit(1)

                    Spacer()

                    if workspace.id == activeWorkspaceID {
                        Image(systemName: "checkmark")
                            .font(.system(size: 14, weight: .medium))
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .font(.system(size: 18, weight: .medium, design: .rounded))
                .padding(5)
                .background {
                    if workspace.id == activeWorkspaceID {
                        RoundedRectangle(cornerRadius: 6, style: .continuous)
                            .fill(darkMode ? Color("AlwaysDarkSecondaryBackground") : Color("AlwaysLightSecondaryBackground"))
                            .matchedGeometryEffect(id: "WORKSPACE", in: animation)
                    }
                }
                .contentShape(Rectangle())
                .onTapGesture {
                    onSelect(workspace.id)
                }
            }
        }
        .foregroundColor(darkMode ? Color("AlwaysLightBackground") : Color("AlwaysDarkBackground"))
        .padding(4)
        .frame(width: 200)
        .background(
            RoundedRectangle(cornerRadius: 9, style: .continuous)
                .fill(darkMode ? Color("AlwaysDarkBackground") : Color("AlwaysLightBackground"))
                .shadow(color: darkMode ? Color.clear : Color.gray.opacity(0.25), radius: 6)
        )
        .overlay(
            RoundedRectangle(cornerRadius: 9, style: .continuous)
                .stroke(darkMode ? Color.gray.opacity(0.1) : Color.clear, lineWidth: 1.3)
        )
    }

    private func workspaceTint(_ workspace: AIConversationWorkspace) -> Color {
        if workspace.bucketID == nil {
            return Color(hex: "4ECDC4")
        }

        if workspace.bucketID == activeBucketID {
            return Color(hex: "7B8FF8")
        }

        return Color(hex: "7B8FF8")
    }
}

private struct ThinkingDisclosure: View {
    let thinking: String
    @State private var isExpanded = false

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(spacing: 6) {
                Image(systemName: "brain")
                    .font(.system(size: 11, weight: .semibold))
                    .foregroundStyle(
                        LinearGradient(
                            colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                            startPoint: .leading, endPoint: .trailing
                        )
                    )
                Text("Reasoning")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .foregroundColor(Color(.secondaryLabel))
                Spacer(minLength: 0)
                Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                    .font(.system(size: 10, weight: .medium))
                    .foregroundColor(Color(.tertiaryLabel))
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .contentShape(Rectangle())
            .onTapGesture {
                withAnimation(.spring(response: 0.28, dampingFraction: 0.8)) {
                    isExpanded.toggle()
                }
            }

            if isExpanded {
                Rectangle()
                    .fill(Color(.separator).opacity(0.4))
                    .frame(height: 1)
                    .padding(.horizontal, 10)

                Text(thinking)
                    .font(.system(.caption, design: .rounded))
                    .foregroundColor(Color(.secondaryLabel))
                    .fixedSize(horizontal: false, vertical: true)
                    .padding(10)
            }
        }
        .background(
            RoundedRectangle(cornerRadius: 10, style: .continuous)
                .fill(Color(.tertiarySystemBackground))
                .overlay(
                    RoundedRectangle(cornerRadius: 10, style: .continuous)
                        .strokeBorder(
                            LinearGradient(
                                colors: [Color(hex: "4ECDC4").opacity(0.35), Color(hex: "9BAAF8").opacity(0.35)],
                                startPoint: .leading, endPoint: .trailing
                            ),
                            lineWidth: 1
                        )
                )
        )
        .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
    }
}

private struct SakuAILandingAction: Identifiable {
    let id = UUID()
    let title: String
    let systemImage: String
    let prompt: String?
}

struct HomeAIAnimatedGradientBackground: View {
    var body: some View {
        ZStack {
            RadialGradient(
                colors: [Color(hex: "4ECDC4").opacity(0.28), Color.clear],
                center: .topLeading,
                startRadius: 0,
                endRadius: 340
            )
            RadialGradient(
                colors: [Color(hex: "9BAAF8").opacity(0.22), Color.clear],
                center: .topTrailing,
                startRadius: 0,
                endRadius: 320
            )
        }
    }
}

private struct SakuAIDarkBackgroundView: View {
    var body: some View {
        ZStack {
            LinearGradient(
                colors: [
                    Color(hex: "07101F"),
                    Color(hex: "0A1530"),
                    Color(hex: "090D1B")
                ],
                startPoint: .top,
                endPoint: .bottom
            )

            RadialGradient(
                colors: [Color(hex: "0DA8C6").opacity(0.55), Color.clear],
                center: .topLeading,
                startRadius: 0,
                endRadius: 420
            )
            .blur(radius: 14)

            RadialGradient(
                colors: [Color(hex: "6544F6").opacity(0.48), Color.clear],
                center: .topTrailing,
                startRadius: 0,
                endRadius: 420
            )
            .blur(radius: 14)

            RadialGradient(
                colors: [Color(hex: "D548B8").opacity(0.34), Color.clear],
                center: UnitPoint(x: 0.82, y: 0.34),
                startRadius: 0,
                endRadius: 320
            )
            .blur(radius: 18)
        }
    }
}
